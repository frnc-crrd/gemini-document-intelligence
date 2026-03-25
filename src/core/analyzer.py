"""Motor de análisis de documentos asistido por Inteligencia Artificial.

Implementa el pipeline Multi-Agente con reglas heurísticas duras derivadas del 
contexto histórico, forzando la inferencia predictiva ante deficiencias de OCR.
Aplica el Cortacircuitos (Circuit Breaker) para detener la fuga financiera.
"""

import time
import random
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
        max_wait = 60.0

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
                error_msg = str(e).upper()
                is_retryable = any(code in error_msg for code in ["429", "RESOURCE_EXHAUSTED", "500", "502", "503", "504"])
                
                if is_retryable:
                    logger.warning(f"Inestabilidad de red o cuota excedida (Intento {attempt + 1}/{settings.max_retries}): {e}")
                    if attempt < settings.max_retries - 1:
                        temp_wait = min(max_wait, base_wait * (2 ** attempt))
                        jitter_wait = random.uniform(0.0, temp_wait)
                        time.sleep(jitter_wait)
                    else:
                        logger.error("Se agotaron los reintentos permitidos. Abortando inferencia.")
                        raise ConnectionAbortedError(f"Bloqueo estricto de proveedor (API Quota): {e}")
                else:
                    logger.error(f"Fallo irrecuperable en la API (Ej. Bad Request / Error de Sintaxis): {e}")
                    break
            except Exception as e:
                logger.error(f"Fallo sistémico no controlado durante la inferencia: {e}", exc_info=True)
                break
                
        return None

    def analyze_batch(self, page_paths: List[Path], original_filename: str) -> AnalysisResponse:
        name_map: Dict[str, str] = {}
        raw_images_clean_names: List[Tuple[str, Image.Image]] = []
        
        # Eliminación de la ordenación alfabética para preservar integridad cronológica pre-calculada
        for idx, p in enumerate(page_paths):
            clean_name = f"page_{idx+1:03d}{p.suffix}"
            name_map[clean_name] = p.name
            raw_images_clean_names.append((clean_name, self._pdf_page_to_image(p)))

        logger.info(f"[Map 1/2] Análisis espacial OCR-Aware -> '{original_filename}' ({len(page_paths)} páginas).")
        
        prompt_espacial = f"""Eres un clasificador espacial de alta precisión y experto en OCR.
Tu única tarea es determinar la rotación física necesaria para que el documento quede perfectamente derecho y legible de arriba hacia abajo y de izquierda a derecha.
Evalúa CADA IMAGEN extraída del archivo '{original_filename}'.

### CONTEXTO DEL DOCUMENTO (ANCLAS DE ORIENTACIÓN ESPACIAL)
Para determinar cuál debe ser el borde "SUPERIOR" (Arriba) de la imagen, busca activamente estas palabras clave exclusivas del ENCABEZADO:
- "PRAC ALIMENTOS SA DE CV", "PROLONGACION AGUSTIN CASTRO 2090", "CENTRO", "GOMEZ PALACIO, DURANGO CP: 35000"
- "RFC: PAL190122D67", "FACTURA", "Fecha", "Folio"
- "Cliente", "Domicilio fiscal:", "Régimen fiscal:"
- "Orden de compra", "Condiciones", "Vendedor", "Vía de embarque"
- Columnas iniciales: "Articulo", "Nombre", "U.med.", "Unidades", "Precio", "Descto.", "Importe"

Para determinar cuál debe ser el borde "INFERIOR" (Abajo) de la imagen, busca activamente estas palabras clave exclusivas del CIERRE o FOOTER:
- "Cadena original del complemento de certificación digital del SAT:"
- "Sello digital del CFDI:", "Sello digital del SAT:", Códigos QR visibles.
- "Subtotal", "002 IVA 0%", "Total(MXN)", "Total(USD)"
- "Método de pago:", "Forma de pago:", "Uso de CFDI:"
- "Este documento es una representación impresa de un CFDI. Folio del SAT:"
- "Fecha de certificación:", "Certificado del emisor:", "Certificado del SAT:", "Régimen fiscal del emisor:", "Lugar de expedición:", "CFDI 4.0/Ingreso"

### POLÍTICA DE SEGURIDAD ESTRICTA (ZERO-TRUST ROTATION)
El 95% de las fotografías e imágenes YA ESTÁN AL DERECHO gracias a los metadatos de las cámaras.
Si la imagen ya es legible de izquierda a derecha (las letras se leen normal), TU OBLIGACIÓN ABSOLUTA ES RESPONDER 0.
Aplica rotación ÚNICAMENTE si tienes evidencia visual irrefutable de que las anclas descritas están en un borde incorrecto. ANTE LA MENOR DUDA, RESPONDE 0.

### GRADOS PERMITIDOS (SENTIDO HORARIO)
- 0: LA IMAGEN YA ESTÁ AL DERECHO. NO REQUIERE GIRO. (Usa este valor por defecto si el texto principal se lee de izquierda a derecha).
- 90: El texto está acostado apuntando hacia la derecha. (Giro 90 grados).
- 180: EL DOCUMENTO ESTÁ TOTALMENTE DE CABEZA. Las letras están invertidas.
- 270: El texto está acostado apuntando hacia la izquierda. (Giro 270 grados).

INSTRUCCIONES:
1. Observa la orientación natural de las letras y busca las anclas de encabezado y footer.
2. Si el documento se puede leer normalmente, RESPONDE 0.
3. Responde ESTRICTAMENTE con: 0, 90, 180 o 270.
"""
        orientacion_result = self._ejecutar_agente(prompt_espacial, raw_images_clean_names, OrientationResponse)
        
        upright_images_dict: Dict[str, Image.Image] = {n: img for n, img in raw_images_clean_names}
        
        if orientacion_result:
            for item in orientacion_result.orientations:
                grados_pdf = item.rotation_degrees
                llm_name = item.file_name.strip().strip("'\"").lower()
                llm_stem = Path(llm_name).stem
                
                matched_clean_name = None
                for n, _ in raw_images_clean_names:
                    if n.lower() == llm_name or Path(n).stem.lower() == llm_stem:
                        matched_clean_name = n
                        break
                        
                if not matched_clean_name:
                    logger.warning(f"El Agente Espacial alucinó el nombre del archivo: {item.file_name}. Se omite rotación.")
                    continue

                if grados_pdf != 0:
                    img = upright_images_dict[matched_clean_name]
                    
                    if grados_pdf == 90: img_enderezada = img.rotate(-90, expand=True)
                    elif grados_pdf == 180: img_enderezada = img.rotate(180, expand=True)
                    elif grados_pdf == 270: img_enderezada = img.rotate(-270, expand=True)
                    else: img_enderezada = img
                    
                    upright_images_dict[matched_clean_name] = img_enderezada

                    real_name = name_map[matched_clean_name]
                    real_path = next((p for p in page_paths if p.name == real_name), None)
                    if real_path: 
                        PDFToolbox.apply_physical_rotation(real_path, grados_pdf)

        upright_images = [(n, upright_images_dict[n]) for n, _ in raw_images_clean_names]

        logger.info(f"[Map 2/2] Auditoría semántica de límites topológicos -> '{original_filename}'.")
        contexto_actual = self.context_manager.obtener_contexto_actual()
        catalogo_tipos = ", ".join([f"'{t}'" for t in contexto_actual.get("categorias_permitidas", [])])
        
        prompt_auditor = f"""Eres un auditor experto en reconstrucción de documentos lógicos, clasificación semántica y extracción de metadatos corporativos.
Las imágenes que estás evaluando ya han sido procesadas por un agente de visión espacial y se encuentran perfectamente enderezadas. 
Tu trabajo es agrupar las páginas que pertenecen al mismo documento, ordenarlas cronológicamente y extraer sus metadatos con máxima precisión.

### HEURÍSTICA DE LÍMITES Y TOPOLOGÍA DEL DOCUMENTO
Aplica ESTRICTAMENTE la siguiente clasificación para identificar en qué parte del documento se encuentra la página actual basándote en su contenido textual:
1. INICIO (Página 1): Cabecera u hoja frontal. Evidencia: "PRAC ALIMENTOS SA DE CV", "RFC: PAL190122D67", "FACTURA", "Folio", "Cliente".
2. CIERRE (Última Página): Calce y totales. Evidencia: "Cadena original del complemento", "Sello digital", "Codigo QR", "Total(MXN)".
3. INTERMEDIA: Desglose atrapado lógicamente entre INICIO y CIERRE. Carece de membrete y de sellos fiscales.
4. UNICA: INICIO y CIERRE conviven en la misma hoja.
5. HUERFANA: Fragmento ilegible sin información semántica suficiente para deducir a qué documento pertenece.

### INSTRUCCIONES DE EXTRACCIÓN Y ENRUTAMIENTO MULTIPLEX:
- Agrupa todas las páginas que correspondan al mismo "Folio".
- El orden del arreglo de nombres de archivos (`ordered_file_names`) para un folio debe ser estrictamente secuencial: INICIO -> [INTERMEDIAS si existen] -> CIERRE.
- Extrae el nombre del Cliente receptor con la mayor fidelidad posible, rastreando los campos "Cliente" u "RFC".
- Categoriza semánticamente el documento en uno de los siguientes tipos estandarizados: [{catalogo_tipos}].

### REGLAS ORO DE DEDUCCIÓN (MITIGACIÓN DE FALLOS DE OCR):
- **PRIORIDAD DE PREFIJO:** Si NO alcanzas a leer la palabra "FACTURA" en el documento por mala calidad de imagen, pero el folio extraído comienza con la letra 'A' o 'P' (Ej: A12052, P543), DEBES catalogarlo invariablemente como "Factura".
- **ENRUTAMIENTO DOBLE:** Si un escaneo agrupa evidencias visuales de dos tipos de documentos distintos (Ej. Una factura engrapada con una nota de remisión), debes asignar el campo `document_type` usando una coma, EXACTAMENTE ASÍ: "Factura, Remisión". Esto permitirá que el sistema lo guarde en ambas carpetas.
"""
        prompt_auditor += "\n\n" + self.context_manager.generar_prompt_contexto()

        extraccion_result = self._ejecutar_agente(prompt_auditor, upright_images, ExtractionResponse)
        final_documents: List[LogicalDocument] = []
        
        if extraccion_result:
            for doc_ext in extraccion_result.documents:
                page_instructions = []
                for fn in doc_ext.ordered_file_names:
                    llm_name = fn.strip().strip("'\"").lower()
                    llm_stem = Path(llm_name).stem
                    
                    matched_clean_name = None
                    for n in name_map.keys():
                        if n.lower() == llm_name or Path(n).stem.lower() == llm_stem:
                            matched_clean_name = n
                            break
                            
                    if matched_clean_name:
                        real_name = name_map[matched_clean_name]
                        page_instructions.append(PageInstruction(file_name=real_name, rotation_degrees=0))

                if not page_instructions:
                    continue

                justificacion = doc_ext.reasoning
                for c_name, r_name in name_map.items(): 
                    justificacion = justificacion.replace(c_name, r_name)
                
                final_doc = LogicalDocument(
                    folios=doc_ext.folios, pages=page_instructions, document_type=doc_ext.document_type,
                    client_name=doc_ext.client_name, confidence_score=doc_ext.confidence_score, reasoning=justificacion
                )
                final_documents.append(final_doc)
        else:
            fallback_pages = [PageInstruction(file_name=real_n, rotation_degrees=0) for _, real_n in name_map.items()]
            final_documents.append(LogicalDocument(
                folios=[settings.error_ilegible], pages=fallback_pages, document_type="No identificado",
                confidence_score=0, reasoning="Fallo de estructuración del pipeline."
            ))

        logger.info(f"[Map Fin] Extracción exitosa: {len(final_documents)} entidad(es) lógica(s) hallada(s) en '{original_filename}'.")
        return AnalysisResponse(documents=final_documents)