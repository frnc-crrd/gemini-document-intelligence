"""Motor de análisis de documentos asistido por IA (Multi-Agente).

Divide la carga cognitiva en dos fases:
1. Normalización espacial mediante "Ancla de Lectura" (A prueba de balas).
2. Extracción, ordenamiento y categorización semántica implacable.
Implementa mapeo de nombres, Apagado de Filtros de Seguridad y Conservación de Masa.
"""

import time
import re
from typing import List, Dict
from pathlib import Path
from PIL import Image

from google import genai
from google.genai import types

from src import config
from src.models import (
    AnalysisResponse, LogicalDocument, PageInstruction,
    OrientationResponse, ExtractionResponse
)
from src.core.context import SystemContextManager


class DocumentAnalyzer:
    """Encargado de la interpretación semántica mediante un pipeline de 2 pasos."""

    def __init__(self):
        self.client = genai.Client(api_key=config.GEMINI_API_KEY)
        self.context_manager = SystemContextManager()
        
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
        import fitz
        doc = fitz.open(str(pdf_path))
        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc.close()
        return img

    def _ejecutar_agente(self, prompt: str, images_with_names: list, schema: any) -> any:
        contents = []
        for name, img in images_with_names:
            contents.append(f"--- ARCHIVO: {name} ---")
            contents.append(img)
        contents.append(prompt)

        base_wait = 2 

        for attempt in range(config.MAX_RETRIES):
            try:
                response = self.client.models.generate_content(
                    model=config.GEMINI_MODEL,
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
                    
            except Exception as e:
                error_msg = str(e)
                print(f"      [!] Error en API (Intento {attempt + 1}/{config.MAX_RETRIES}): {error_msg}")
                wait_time = base_wait * (2 ** attempt)
                
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    match = re.search(r'retry in ([\d\.]+)s', error_msg)
                    if match:
                        wait_time = int(float(match.group(1))) + 2
                
                print(f"      [~] Aplicando protocolo de resiliencia. Pausando por {wait_time}s...")
                time.sleep(wait_time)
                
        return None

    def analyze_batch(self, page_paths: List[Path], original_filename: str) -> AnalysisResponse:
        sorted_paths = sorted(page_paths)
        name_map = {}
        raw_images_clean_names = []
        
        for idx, p in enumerate(sorted_paths):
            clean_name = f"page_{idx+1:03d}{p.suffix}"
            name_map[clean_name] = p.name
            img = self._pdf_page_to_image(p)
            raw_images_clean_names.append((clean_name, img))

        # =========================================================
        # FASE 1: AGENTE ESPACIAL (Ancla de Encabezado)
        # =========================================================
        print("    -> Agente 1: Analizando orientación espacial...")
        
        prompt_espacial = f"""Eres un clasificador visual extremadamente básico.
        Tu ÚNICA tarea es decirme en qué parte de la imagen está el ENCABEZADO PRINCIPAL (el título del documento, el logo de la empresa, o el inicio de la tabla).
        Evalúa CADA IMAGEN de '{original_filename}' independientemente. No te dejes engañar si la imagen es muy ancha o muy alta.
        
        Responde ESTRICTAMENTE con una de estas 4 opciones para `orientacion` basándote en dónde está el encabezado:
        
        - "NORMAL": El encabezado o inicio del texto está en la parte de ARRIBA de la imagen. (La hoja está derecha. Si es una tabla ancha pero el título está arriba, es NORMAL).
        - "ACOSTADO_IZQUIERDA": El encabezado está pegado al lado IZQUIERDO de la imagen. (La hoja está rotada).
        - "ACOSTADO_DERECHA": El encabezado está pegado al lado DERECHO de la imagen. (La hoja está rotada).
        - "INVERTIDO": El encabezado está en la parte de ABAJO de la imagen. (La hoja está de cabeza).
        """

        orientacion_result = self._ejecutar_agente(prompt_espacial, raw_images_clean_names, OrientationResponse)
        
        rotation_degrees_map = {}
        upright_images = []
        
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
            upright_images = raw_images_clean_names
            rotation_degrees_map = {name_map[n]: 0 for n, _ in raw_images_clean_names}

        # =========================================================
        # FASE 2: AGENTE AUDITOR (Lógica Intacta)
        # =========================================================
        print("    -> Agente 2: Ejecutando auditoría semántica...")
        contexto_actual = self.context_manager.obtener_contexto_actual()
        catalogo_tipos = ", ".join([f"'{t}'" for t in contexto_actual.get("categorias_permitidas", [])])
        
        prompt_auditor = f"""Eres un auditor experto. Las imágenes que estás viendo YA FUERON ENDEREZADAS. 
        Tu tarea es armar el rompecabezas visual de estos documentos:
        
        1. CONSERVACIÓN DE MASA (CRÍTICO): Tienes que asignar TODAS Y CADA UNA de las imágenes que recibiste a un documento. NINGUNA página puede quedar omitida o fuera de tu JSON final.

        2. PROHIBIDO USAR EL UUID (CRÍTICO): Extrae el folio principal de la factura o remisión. ESTÁ ESTRICTAMENTE PROHIBIDO utilizar el "Folio del SAT" (la cadena de 36 caracteres como F40CB1A8-...) como el folio del documento. Si una imagen SOLO tiene sellos, NO uses el UUID como folio.
        
        3. EMPAREJAMIENTO VISUAL INTELIGENTE: Tienes un lote de imágenes desordenadas. NUNCA confíes en los nombres de archivo.
           Si ves una imagen que es claramente la mitad inferior de una factura (puros sellos, totales, QR), DEBES usar tu razonamiento lógico para emparejarla con la imagen que contenga el encabezado y folio correspondiente. 
           Agrupa ambas imágenes en un solo objeto `LogicalDocExtraction` bajo el mismo folio principal. ¡No dejes páginas de sellos "huérfanas"!
           
        4. CATEGORIZACIÓN: Lee el encabezado. Si dice "REMISIÓN", es 'Remisión'. Si dice "Factura", es 'Factura'.
           REGLA DE SALVACAIDAS: Si no tiene título, usa la primera letra del folio extraído: 'R'='Remisión', 'A' o 'P'='Factura', 'O'='Orden de compra'. Usa: [{catalogo_tipos}].

        5. SEPARACIÓN IMPLACABLE: Si ves folios distintos completos, sepáralos en objetos distintos.
        
        6. EXTRACCIÓN MÚLTIPLE: Solo para tablas tituladas "Diarios de ventas", extrae todos los folios impresos.
        
        7. ORDENAMIENTO DE PÁGINAS: PRIMERO la página con el membrete. AL FINAL la página con el código QR y sellos.
           
        8. JUSTIFICACIÓN: Escribe ESTRICTAMENTE EN ESPAÑOL. Sé detallado explicando por qué emparejaste las páginas visualmente.
        """

        prompt_auditor += "\n\n" + self.context_manager.generar_prompt_contexto()

        extraccion_result = self._ejecutar_agente(prompt_auditor, upright_images, ExtractionResponse)

        # =========================================================
        # FASE 3: ENSAMBLAJE FINAL (Lógica Intacta)
        # =========================================================
        final_documents = []
        if extraccion_result:
            for doc_ext in extraccion_result.documents:
                page_instructions = []
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
            fallback_pages = [PageInstruction(file_name=real_n, rotation_degrees=0) for clean_n, real_n in name_map.items()]
            final_documents.append(LogicalDocument(
                folios=[config.ERROR_ILEGIBLE],
                pages=fallback_pages,
                document_type="No identificado",
                client_name=None,
                confidence_score=0,
                reasoning="Fallo total del pipeline multi-agente."
            ))

        return AnalysisResponse(documents=final_documents)