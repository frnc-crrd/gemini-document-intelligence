"""Orquestador principal del pipeline de integridad documental.

Coordina la ingesta, explosión, análisis contextual paralelo y ensamblaje final.
Implementa procesamiento multihilo para aislar la latencia de red y operaciones
de disco. Mantiene el mecanismo "Recogedor de Huérfanos" para garantizar el
principio de conservación de masa documental.
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import get_settings
from src.core.logger import get_system_logger
from src.core.analyzer import DocumentAnalyzer
from src.utils.pdf_tools import PDFToolbox

logger = get_system_logger(__name__)
settings = get_settings()


class PipelineProcessor:
    """Controlador central del flujo de trabajo documental."""

    def __init__(self) -> None:
        self._ensure_directories()
        self.analyzer = DocumentAnalyzer()
        self.max_workers = 4  # Límite de concurrencia optimizado para I/O

    def _ensure_directories(self) -> None:
        """Garantiza la existencia de la estructura de directorios base."""
        for directory in [settings.raw_dir, settings.explosion_dir, settings.final_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def _sanitize_folder_name(self, name: str) -> str:
        """Limpia caracteres inválidos para nombres de directorios en el sistema de archivos."""
        s = re.sub(r'[^\w\s-]', '', name).strip()
        return re.sub(r'[\s]+', '_', s)

    def run(self) -> List[Dict[str, Any]]:
        """Inicia la ejecución del pipeline completo de manera concurrente.

        Returns:
            Lista de diccionarios con la metadata tabular de resultados.
        """
        raw_files = [f for f in settings.raw_dir.iterdir() if f.is_file()]
        all_results: List[Dict[str, Any]] = []
        
        if not raw_files:
            logger.info(f"Directorio de ingesta vacío: {settings.raw_dir}. Abortando procesamiento.")
            return all_results

        logger.info(f"Iniciando procesamiento masivo de {len(raw_files)} archivo(s) físico(s).")

        pdf_files = [f for f in raw_files if f.suffix.lower() == '.pdf']
        image_files = [f for f in raw_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png']]

        # Procesamiento concurrente de PDFs empleando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pdf = {
                executor.submit(self._process_single_pdf, pdf): pdf 
                for pdf in pdf_files
            }
            
            for future in as_completed(future_to_pdf):
                pdf_path = future_to_pdf[future]
                try:
                    file_results = future.result()
                    all_results.extend(file_results)
                except Exception as e:
                    logger.error(f"Fallo catastrófico en el hilo procesando {pdf_path.name}: {e}", exc_info=True)

        # Procesamiento en bloque para las imágenes sueltas
        if image_files:
            try:
                file_results = self._process_loose_images(image_files)
                all_results.extend(file_results)
            except Exception as e:
                logger.error(f"Fallo catastrófico al procesar lote de imágenes: {e}", exc_info=True)

        logger.info(f"Procesamiento masivo completado. Total de entidades lógicas generadas: {len(all_results)}.")
        return all_results

    def _process_single_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        """Aísla la ejecución de un archivo PDF único.

        Args:
            file_path: Ruta al archivo físico en el directorio de ingesta.

        Returns:
            Lista de diccionarios con el resultado de la extracción.
        """
        logger.info(f"Iniciando explosión física del PDF: {file_path.name}")
        try:
            exploded_paths = PDFToolbox.explode_pdf(file_path, settings.explosion_dir)
            if not exploded_paths: 
                return []
            
            return self._execute_ai_pipeline(
                exploded_paths=exploded_paths, 
                original_file_name=file_path.name, 
                original_file_type="PDF"
            )
        except Exception as e:
            logger.error(f"Abortando procesamiento del archivo {file_path.name} debido a error: {e}")
            return []

    def _process_loose_images(self, image_paths: List[Path]) -> List[Dict[str, Any]]:
        """Empaqueta imágenes sueltas en formato PDF y orquesta su análisis.

        Args:
            image_paths: Lista de rutas físicas de las imágenes.

        Returns:
            Lista de diccionarios con el resultado de la extracción.
        """
        logger.info(f"Procesando lote de {len(image_paths)} imágenes sueltas.")
        exploded_paths: List[Path] = []
        
        for p in image_paths:
            try:
                wrapped = PDFToolbox.wrap_image_to_pdf(p, settings.explosion_dir)
                exploded_paths.append(wrapped)
            except Exception as e:
                logger.warning(f"Omitiendo imagen ilegible {p.name}: {e}")
                
        if not exploded_paths: 
            return []
        
        return self._execute_ai_pipeline(
            exploded_paths=exploded_paths, 
            original_file_name="LOTE_FOTOGRAFICO_SUELTO", 
            original_file_type="IMÁGENES MIXTAS"
        )

    def _execute_ai_pipeline(self, exploded_paths: List[Path], original_file_name: str, original_file_type: str) -> List[Dict[str, Any]]:
        """Maneja la inferencia de IA y el reensamblaje físico del documento.

        Implementa el algoritmo de "Recogedor de Huérfanos" para garantizar que
        ningún fragmento procesado quede excluido del almacenamiento final.

        Args:
            exploded_paths: Rutas físicas de las páginas fragmentadas.
            original_file_name: Identificador del archivo de origen.
            original_file_type: Clasificador de origen.

        Returns:
            Listado de resultados tabulares.
        """
        batch_results: List[Dict[str, Any]] = []
        original_page_count = len(exploded_paths)
        exploded_paths.sort()
        path_map = {p.name: p for p in exploded_paths}

        logger.info(f"Enviando lote de {original_page_count} páginas a motor de inferencia. Origen: {original_file_name}")
        analysis_result = self.analyzer.analyze_batch(exploded_paths, original_file_name)

        used_pages: Set[str] = set()

        for doc in analysis_result.documents:
            pages_data: List[Dict[str, Any]] = []
            for page_instruction in doc.pages:
                clean_filename = page_instruction.file_name.strip().strip("'\"")
                if clean_filename in path_map:
                    pages_data.append({
                        "path": path_map[clean_filename],
                        "rotation": page_instruction.rotation_degrees
                    })
                    used_pages.add(clean_filename)

            if not pages_data:
                continue

            final_page_count = len(pages_data)
            category_folder = self._sanitize_folder_name(doc.document_type)
            category_dir = settings.final_dir / category_folder
            category_dir.mkdir(exist_ok=True, parents=True)

            status = "OK" if doc.confidence_score > 80 else "REVISIÓN MANUAL"
            
            for folio in doc.folios:
                try:
                    final_pdf_path = PDFToolbox.merge_by_folio(
                        pages_data=pages_data,
                        folio=folio,
                        output_dir=category_dir
                    )
                    
                    ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                    logger.debug(f"Documento consolidado y almacenado: {ruta_relativa}")
                    
                    batch_results.append({
                        "Folio": folio,
                        "Categoría": doc.document_type,
                        "Cliente": doc.client_name or "NO DETECTADO",
                        "Archivo Original": original_file_name,
                        "Tipo Original": original_file_type,
                        "Páginas Original": original_page_count,
                        "Páginas Final": final_page_count,
                        "Status": status,
                        "Confianza": doc.confidence_score,
                        "Ruta del Archivo": ruta_relativa,
                        "Justificación": doc.reasoning
                    })
                except Exception as e:
                    logger.error(f"Fallo al ensamblar documento final para folio {folio}: {e}")

        # ==========================================
        # RECOGEDOR DE HUÉRFANOS (CONSERVACIÓN DE MASA)
        # ==========================================
        orphan_files = set(path_map.keys()) - used_pages
        if orphan_files:
            logger.warning(f"Conservación de masa activada. Rescatando {len(orphan_files)} fragmentos huérfanos de {original_file_name}.")
            category_folder = "Huerfanos_Rebotes"
            category_dir = settings.final_dir / category_folder
            category_dir.mkdir(exist_ok=True, parents=True)
            
            for orphan in orphan_files:
                try:
                    pages_data = [{"path": path_map[orphan], "rotation": 0}]
                    final_pdf_path = PDFToolbox.merge_by_folio(
                        pages_data=pages_data,
                        folio=f"HUERFANO_{Path(orphan).stem}",
                        output_dir=category_dir
                    )
                    
                    ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                    
                    batch_results.append({
                        "Folio": settings.error_ilegible,
                        "Categoría": "Página Huérfana",
                        "Cliente": "NO DETECTADO",
                        "Archivo Original": original_file_name,
                        "Tipo Original": original_file_type,
                        "Páginas Original": original_page_count,
                        "Páginas Final": 1,
                        "Status": "REVISIÓN MANUAL",
                        "Confianza": 0,
                        "Ruta del Archivo": ruta_relativa,
                        "Justificación": "Fragmento excluido por la inferencia. Rescatado por política de conservación de masa."
                    })
                except Exception as e:
                    logger.error(f"Fallo al rescatar fragmento huérfano {orphan}: {e}")

        return batch_results