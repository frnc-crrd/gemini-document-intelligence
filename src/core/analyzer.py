"""Motor de análisis de documentos asistido por Inteligencia Artificial.

Implementa el pipeline Multi-Agente con heurísticas y mapeo visual estricto
para evadir alucinaciones algorítmicas de la inferencia del modelo LLM.
"""

import time
import re
from typing import List, Dict, Tuple, TypeVar, Type, Optional
from pathlib import Path
from PIL import Image
from google import genai
from google.genai import types
from google.genai.errors import APIError

from src.config import get_settings
from src.core.logger import get_system_logger
from src.core.context import get_context_manager
from src.utils.pdf_tools import PDFToolbox
from src.models import (
    AnalysisResponse, LogicalDocument, PageInstruction,
    OrientationResponse, ExtractionResponse
)

logger = get_system_logger(__name__)
settings = get_settings()

T = TypeVar('T')

class DocumentAnalyzer:
    """Clase principal para la orquestación de inferencia y extracción documental."""

    def __init__(self) -> None:
        self.client = genai.Client(api_key=settings.gemini_api_key)
        self.context_manager = get_context_manager()
        
        self.safety_settings = [
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
        ]

    def _pdf_page_to_image(self, pdf_path: Path) -> Image.Image:
        import fitz
        try:
            with fitz.open(str(pdf_path)) as doc:
                page = doc[0]
                pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
                return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        except Exception as e:
            raise RuntimeError(f"Imposible extraer imagen de {pdf_path}") from e

    def _ejecutar_agente(self, prompt: str, images_with_names: List[Tuple[str, Image.Image]], schema: Type[T]) -> Optional[T]:
        contents = []
        for name, img in images_with_names:
            contents.append(f"--- ARCHIVO: {name} ---")
            contents.append(img)
        contents.append(prompt)

        base_wait = settings.api_delay

        for attempt in range(settings.max_retries):
            try:
                response = self.client.models.generate_content(
                    model=settings.gemini_model,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema,
                        temperature=0.0,
                        safety_settings=self.safety_settings
                    )
                )
                if response.parsed:
                    return response.parsed
            except APIError as e:
                error_msg = str(e)
                logger.warning(f"Error en API (Intento {attempt + 1}/{settings.max_retries}): {error_msg}")
                wait_time = base_wait * (2 ** attempt)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    match = re.search(r'retry in ([\d\.]+)s', error_msg)
                    if match: wait_time = float(match.group(1)) + 2.0
                time.sleep(wait_time)
            except Exception:
                break
                
        return None

    def analyze_batch(self, page_paths: List[Path], original_filename: str) -> AnalysisResponse:
        sorted_paths = sorted(page_paths)
        name_map: Dict[str, str] = {}
        raw_images_clean_names: List[Tuple[str, Image.Image]] = []
        
        for idx, p in enumerate(sorted_paths):
            clean_name = f"page_{idx+1:03d}{p.suffix}"
            name_map[clean_name] = p.name
            raw_images_clean_names.append((clean_name, self._pdf_page_to_image(p)))

        logger.info(f"Fase 1 iniciada: Análisis OCR-Aware espacial para {original_filename}")
        
        prompt_espacial = f"""Eres un clasificador espacial de alta precisión.
Tu tarea es determinar la rotación necesaria para que el documento sea legible (texto normal).
Evalúa CADA IMAGEN de '{original_filename}'.

HEURÍSTICA DE ROTACIÓN ESTRICTA (Grados a aplicar en SENTIDO HORARIO para arreglar la imagen):
- 0: La imagen ya está perfecta. El texto se lee normal de izquierda a derecha.
- 90: El texto está acostado. Las letras van de ABAJO hacia ARRIBA (Si lo ves de frente, tienes que torcer la cabeza a la izquierda para leer).
- 180: El documento está totalmente DE CABEZA (texto invertido).
- 270: El texto está acostado. Las letras van de ARRIBA hacia ABAJO (Tienes que torcer la cabeza a la derecha para leer).

INSTRUCCIONES:
1. Identifica hacia dónde "apunta" la parte superior del texto (anclas como RFC, tablas, logos).
2. Responde ESTRICTAMENTE con: 0, 90, 180 o 270.
"""
        orientacion_result = self._ejecutar_agente(prompt_espacial, raw_images_clean_names, OrientationResponse)
        
        rotation_degrees_map: Dict[str, int] = {}
        upright_images: List[Tuple[str, Image.Image]] = []
        
        if orientacion_result:
            for item in orientacion_result.orientations:
                grados_pdf = item.rotation_degrees
                clean_name = item.file_name
                try:
                    img = next(i for n, i in raw_images_clean_names if n == clean_name)
                except StopIteration:
                    img = raw_images_clean_names[0][1]
                    clean_name = raw_images_clean_names[0][0]

                real_name = name_map.get(clean_name, clean_name)
                
                if grados_pdf != 0:
                    if grados_pdf == 90: img_enderezada = img.rotate(-90, expand=True)
                    elif grados_pdf == 180: img_enderezada = img.rotate(180, expand=True)
                    elif grados_pdf == 270: img_enderezada = img.rotate(-270, expand=True)
                    else: img_enderezada = img

                    real_path = next((p for p in sorted_paths if p.name == real_name), None)
                    if real_path: PDFToolbox.apply_physical_rotation(real_path, grados_pdf)
                else:
                    img_enderezada = img

                rotation_degrees_map[real_name] = 0
                upright_images.append((clean_name, img_enderezada))
        else:
            upright_images = raw_images_clean_names
            rotation_degrees_map = {name_map[n]: 0 for n, _ in raw_images_clean_names}

        logger.info("Fase 2 iniciada: Ejecutando heurística de límites y auditoría semántica...")
        contexto_actual = self.context_manager.obtener_contexto_actual()
        catalogo_tipos = ", ".join([f"'{t}'" for t in contexto_actual.get("categorias_permitidas", [])])
        
        prompt_auditor = f"""Eres un auditor experto en reconstrucción de documentos lógicos.
Las imágenes que estás evaluando ya se encuentran enderezadas. Debes agruparlas y ordenarlas aplicando ESTRICTAMENTE la siguiente HEURÍSTICA DE LÍMITES:
1. INICIO: Contiene leyenda principal (Factura, Remisión) y Folio.
2. CIERRE: Evaluando el footer. Contiene información fiscal (Sellos, QR) o firmas.
3. INTERMEDIA: Atrapada entre INICIO y CIERRE.
4. UNICA: INICIO y CIERRE conviven en la misma página física.
5. HUERFANA: Fragmento ilegible.

INSTRUCCIONES:
- Asigna TODAS las imágenes.
- NUNCA extraigas la cadena de 36 caracteres del SAT como folio.
- Categoriza en: [{catalogo_tipos}].
"""
        prompt_auditor += "\n\n" + self.context_manager.generar_prompt_contexto()

        extraccion_result = self._ejecutar_agente(prompt_auditor, upright_images, ExtractionResponse)
        final_documents: List[LogicalDocument] = []
        
        if extraccion_result:
            for doc_ext in extraccion_result.documents:
                page_instructions = [
                    PageInstruction(file_name=name_map.get(fn, fn), rotation_degrees=rotation_degrees_map.get(name_map.get(fn, fn), 0))
                    for fn in doc_ext.ordered_file_names
                ]
                justificacion = doc_ext.reasoning
                for c_name, r_name in name_map.items(): justificacion = justificacion.replace(c_name, r_name)
                
                final_doc = LogicalDocument(
                    folios=doc_ext.folios, pages=page_instructions, document_type=doc_ext.document_type,
                    client_name=doc_ext.client_name, confidence_score=doc_ext.confidence_score, reasoning=justificacion
                )
                final_documents.append(final_doc)
        else:
            fallback_pages = [PageInstruction(file_name=real_n, rotation_degrees=0) for _, real_n in name_map.items()]
            final_documents.append(LogicalDocument(
                folios=[settings.error_ilegible], pages=fallback_pages, document_type="No identificado",
                confidence_score=0, reasoning="Fallo del pipeline."
            ))

        return AnalysisResponse(documents=final_documents)