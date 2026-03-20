"""Motor de análisis de documentos asistido por Inteligencia Artificial.

Implementa el pipeline Multi-Agente (Espacial y Auditor) asegurando la inferencia
estricta de tipos. Integra manejo robusto de excepciones y backoff exponencial
para tolerancia a fallos ante límites de cuota de la API externa.
"""

import time
import re
from typing import List, Dict, Tuple, TypeVar, Type, Optional
from pathlib import Path

# Captura de errores de Pillow para asegurar resiliencia en manejo de imágenes
from PIL import Image, UnidentifiedImageError

from google import genai
from google.genai import types
from google.genai.errors import APIError

from src.config import get_settings
from src.core.logger import get_system_logger
from src.core.context import get_context_manager
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
        
        # Apagado explícito de filtros de seguridad para procesar documentos legales/comerciales 
        # que puedan contener falsos positivos por terminología técnica.
        self.safety_settings = [
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=types.HarmBlockThreshold.BLOCK_NONE,
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE,
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE,
            ),
            types.SafetySetting(
                category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=types.HarmBlockThreshold.BLOCK_NONE,
            ),
        ]

    def _pdf_page_to_image(self, pdf_path: Path) -> Image.Image:
        """Rasteriza una página PDF a un objeto Image de Pillow.
        
        Args:
            pdf_path: Ruta al archivo PDF físico.
            
        Returns:
            Instancia Image generada.
            
        Raises:
            RuntimeError: Si la rasterización falla debido a corrupción de archivo.
        """
        import fitz  # PyMuPDF
        
        try:
            with fitz.open(str(pdf_path)) as doc:
                page = doc[0]
                pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                return img
        except Exception as e:
            logger.error(f"Fallo al rasterizar el PDF {pdf_path}: {e}", exc_info=True)
            raise RuntimeError(f"Imposible extraer imagen de {pdf_path}") from e

    def _ejecutar_agente(self, prompt: str, images_with_names: List[Tuple[str, Image.Image]], schema: Type[T]) -> Optional[T]:
        """Ejecuta una inferencia estructurada contra el LLM aplicando resiliencia.

        Args:
            prompt: Instrucción principal para el agente.
            images_with_names: Lista de tuplas conteniendo el identificador y la imagen rasterizada.
            schema: Clase Pydantic que define el contrato de respuesta esperado.

        Returns:
            Instancia del esquema poblado por el LLM, o None si agota reintentos.
        """
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
                    if match:
                        wait_time = float(match.group(1)) + 2.0
                
                logger.info(f"Aplicando protocolo de backoff exponencial. Pausando {wait_time:.2f}s")
                time.sleep(wait_time)
            except Exception as e:
                # Interceptamos fallos críticos no relacionados con cuotas (ej. desconexión total de red)
                logger.error(f"Fallo crítico e inesperado al comunicar con la API: {e}", exc_info=True)
                break
                
        logger.error("Se agotaron los reintentos de comunicación con el LLM.")
        return None

    def analyze_batch(self, page_paths: List[Path], original_filename: str) -> AnalysisResponse:
        """Orquesta las fases espacial y semántica para un lote de documentos.

        Args:
            page_paths: Lista de rutas físicas de las páginas a procesar.
            original_filename: Nombre del archivo consolidado para trazabilidad.

        Returns:
            Objeto AnalysisResponse conteniendo la extracción lógica validada.
        """
        sorted_paths = sorted(page_paths)
        name_map: Dict[str, str] = {}
        raw_images_clean_names: List[Tuple[str, Image.Image]] = []
        
        for idx, p in enumerate(sorted_paths):
            clean_name = f"page_{idx+1:03d}{p.suffix}"
            name_map[clean_name] = p.name
            img = self._pdf_page_to_image(p)
            raw_images_clean_names.append((clean_name, img))

        # =========================================================
        # FASE 1: AGENTE ESPACIAL (Ancla de Encabezado)
        # =========================================================
        logger.info(f"Fase 1 iniciada: Analizando orientación espacial para {original_filename}")
        
        prompt_espacial = f"""Eres un clasificador visual extremadamente básico.
Tu ÚNICA tarea es decirme en qué parte de la imagen está el ENCABEZADO PRINCIPAL (el título del documento, el logo de la empresa, o el inicio de la tabla).
Evalúa CADA IMAGEN de '{original_filename}' independientemente. No te dejes engañar si la imagen es muy ancha o muy alta.
Responde ESTRICTAMENTE con una de estas 4 opciones para `orientacion` basándote en dónde está el encabezado:
        
        - "NORMAL": El encabezado o inicio del texto está en la parte de ARRIBA de la imagen. (La hoja está derecha).
        - "ACOSTADO_IZQUIERDA": El encabezado está pegado al lado IZQUIERDO de la imagen. (La hoja está rotada).
        - "ACOSTADO_DERECHA": El encabezado está pegado al lado DERECHO de la imagen. (La hoja está rotada).
        - "INVERTIDO": El encabezado está en la parte de ABAJO de la imagen. (La hoja está de cabeza).
"""

        orientacion_result = self._ejecutar_agente(prompt_espacial, raw_images_clean_names, OrientationResponse)
        
        rotation_degrees_map: Dict[str, int] = {}
        upright_images: List[Tuple[str, Image.Image]] = []
        
        if orientacion_result:
            for item in orientacion_result.orientations:
                ori = item.orientacion.strip().upper()
                clean_name = item.file_name
                
                try:
                    img = next(i for n, i in raw_images_clean_names if n == clean_name)
                except StopIteration:
                    img = raw_images_clean_names[0][1]
                    clean_name = raw_images_clean_names[0][0]

                if ori == 'ACOSTADO_IZQUIERDA':
                    img_enderezada = img.rotate(-90, expand=True)
                    grados_pdf = 90
                elif ori == 'ACOSTADO_DERECHA':
                    img_enderezada = img.rotate(90, expand=True)
                    grados_pdf = 270
                elif ori == 'INVERTIDO':
                    img_enderezada = img.rotate(180, expand=True)
                    grados_pdf = 180
                else:
                    img_enderezada = img
                    grados_pdf = 0
                
                real_name = name_map.get(clean_name, clean_name)
                rotation_degrees_map[real_name] = grados_pdf
                upright_images.append((clean_name, img_enderezada))
        else:
            logger.warning("Fallo en Agente Espacial. Asumiendo rotación 0 para todas las páginas.")
            upright_images = raw_images_clean_names
            rotation_degrees_map = {name_map[n]: 0 for n, _ in raw_images_clean_names}

        # =========================================================
        # FASE 2: AGENTE AUDITOR (Extracción Lógica)
        # =========================================================
        logger.info("Fase 2 iniciada: Ejecutando auditoría semántica...")
        contexto_actual = self.context_manager.obtener_contexto_actual()
        catalogo_tipos = ", ".join([f"'{t}'" for t in contexto_actual.get("categorias_permitidas", [])])
        
        prompt_auditor = f"""Eres un auditor experto.
Las imágenes que estás viendo YA FUERON ENDEREZADAS. Tu tarea es armar el rompecabezas visual de estos documentos:
        
1. CONSERVACIÓN DE MASA (CRÍTICO): Tienes que asignar TODAS Y CADA UNA de las imágenes que recibiste a un documento. NINGUNA página puede quedar omitida.
2. PROHIBIDO USAR EL UUID (CRÍTICO): Extrae el folio principal de la factura o remisión. ESTÁ ESTRICTAMENTE PROHIBIDO utilizar el "Folio del SAT" (la cadena de 36 caracteres como F40CB1A8-...) como el folio del documento. Si una imagen SOLO tiene sellos, NO uses el UUID como folio.
3. EMPAREJAMIENTO VISUAL INTELIGENTE: Nunca confíes en los nombres de archivo. Si ves una imagen que es claramente la mitad inferior de una factura (puros sellos, totales, QR), DEBES usar tu razonamiento lógico para emparejarla con la imagen que contenga el encabezado y folio correspondiente. Agrupa ambas imágenes en un solo objeto `LogicalDocExtraction` bajo el mismo folio principal.
4. CATEGORIZACIÓN: Lee el encabezado. Si dice "REMISIÓN", es 'Remisión'. Si dice "Factura", es 'Factura'. Usa el salvavidas de inicial de folio si no hay título. Usa: [{catalogo_tipos}].
5. SEPARACIÓN IMPLACABLE: Si ves folios distintos completos, sepáralos en objetos distintos.
6. EXTRACCIÓN MÚLTIPLE: Solo para tablas tituladas "Diarios de ventas", extrae todos los folios impresos.
7. ORDENAMIENTO DE PÁGINAS: PRIMERO la página con el membrete. AL FINAL la página con el código QR y sellos.
8. JUSTIFICACIÓN: Escribe ESTRICTAMENTE EN ESPAÑOL explicando por qué emparejaste las páginas.
"""
        prompt_auditor += "\n\n" + self.context_manager.generar_prompt_contexto()

        extraccion_result = self._ejecutar_agente(prompt_auditor, upright_images, ExtractionResponse)

        # =========================================================
        # FASE 3: ENSAMBLAJE FINAL
        # =========================================================
        final_documents: List[LogicalDocument] = []
        
        if extraccion_result:
            for doc_ext in extraccion_result.documents:
                page_instructions: List[PageInstruction] = []
                for clean_fname in doc_ext.ordered_file_names:
                    real_fname = name_map.get(clean_fname, clean_fname)
                    page_instructions.append(PageInstruction(
                        file_name=real_fname,
                        rotation_degrees=rotation_degrees_map.get(real_fname, 0)
                    ))
                
                justificacion_limpia = doc_ext.reasoning
                for clean_name, real_name in name_map.items():
                    justificacion_limpia = justificacion_limpia.replace(clean_name, real_name)
                
                final_doc = LogicalDocument(
                    folios=doc_ext.folios,
                    pages=page_instructions,
                    document_type=doc_ext.document_type,
                    client_name=doc_ext.client_name,
                    confidence_score=doc_ext.confidence_score,
                    reasoning=justificacion_limpia
                )
                final_documents.append(final_doc)
                self.context_manager.actualizar_contexto(final_doc)
        else:
            logger.error("Fallo total del pipeline Multi-Agente. Retornando documento de fallback.")
            fallback_pages = [PageInstruction(file_name=real_n, rotation_degrees=0) for _, real_n in name_map.items()]
            final_documents.append(LogicalDocument(
                folios=[settings.error_ilegible],
                pages=fallback_pages,
                document_type="No identificado",
                client_name=None,
                confidence_score=0,
                reasoning="Fallo total del pipeline multi-agente. Documento ilocalizable en parámetros de cuota."
            ))

        return AnalysisResponse(documents=final_documents)