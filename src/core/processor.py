"""Orquestador principal del pipeline de integridad documental.

Coordina la ingesta, explosión, análisis contextual paralelo y ensamblaje final.
Implementa procesamiento multihilo, balanceo de carga para imágenes fotográficas
sueltas y sincronización con el repositorio de base de datos para blindar 
el flujo de I/O de archivos.
"""

import re
import uuid
from pathlib import Path
from typing import List, Dict, Any, Set, Optional, Protocol
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz

from src.config import get_settings
from src.core.logger import get_system_logger
from src.utils.pdf_tools import PDFToolbox
from src.db.repository import PostgresRepository
from src.models import AnalysisResponse

logger = get_system_logger(__name__)
settings = get_settings()


class IDocumentAnalyzer(Protocol):
    """Contrato estricto (Interfaz) para cualquier motor de inferencia analítica."""
    def analyze_batch(self, page_paths: List[Path], original_filename: str) -> AnalysisResponse:
        ...


class PipelineProcessor:
    """Controlador central del flujo de trabajo documental."""

    # Modificación: Inyección de Dependencias en el constructor
    def __init__(self, analyzer: IDocumentAnalyzer, db_repo: Optional[PostgresRepository] = None) -> None:
        self._ensure_directories()
        self.analyzer = analyzer
        self.db_repo = db_repo or PostgresRepository()
        self.max_workers = settings.max_threads

    def _ensure_directories(self) -> None:
        for directory in [settings.raw_dir, settings.explosion_dir, settings.final_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def _sanitize_folder_name(self, name: str) -> str:
        s = re.sub(r'[^\w\s-]', '', name).strip()
        return re.sub(r'[\s]+', '_', s)

    def _get_safe_category_dir(self, category_name: str) -> Path:
        """Resuelve y valida la ruta de destino mitigando vulnerabilidades de Path Traversal."""
        category_folder = self._sanitize_folder_name(category_name)
        category_dir = (settings.final_dir / category_folder).resolve()
        
        if not category_dir.is_relative_to(settings.final_dir.resolve()):
            logger.critical(f"Intento de Path Traversal detectado y bloqueado para categoría: {category_name}")
            raise ValueError(f"Ruta de destino inválida o maliciosa: {category_dir}")
            
        return category_dir

    def run(self) -> List[Dict[str, Any]]:
        raw_files = [f for f in settings.raw_dir.iterdir() if f.is_file()]
        all_results: List[Dict[str, Any]] = []
        
        if not raw_files:
            return all_results

        logger.info(f"Iniciando procesamiento masivo de {len(raw_files)} archivo(s) físico(s).")

        pdf_files = [f for f in raw_files if f.suffix.lower() == '.pdf']
        image_files = [f for f in raw_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png']]

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

        if image_files:
            try:
                file_results = self._process_loose_images(image_files)
                all_results.extend(file_results)
            except Exception as e:
                logger.error(f"Fallo al procesar lote de imágenes: {e}", exc_info=True)

        logger.info(f"Procesamiento masivo completado. Total de entidades lógicas generadas: {len(all_results)}.")
        return all_results

    def _process_single_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        logger.info(f"Iniciando explosión física del PDF: {file_path.name}")
        try:
            exploded_paths = PDFToolbox.explode_pdf(file_path, settings.explosion_dir)
            if not exploded_paths: 
                return []
            return self._execute_ai_pipeline(exploded_paths, file_path.name, "PDF")
        except fitz.FileDataError as e:
            logger.error(f"Estructura de PDF corrupta o ilegible al procesar {file_path.name}: {e}")
            return []
        except OSError as e:
            logger.error(f"Error de sistema operativo (I/O) al acceder a {file_path.name}: {e}")
            return []

    def _process_loose_images(self, image_paths: List[Path]) -> List[Dict[str, Any]]:
        """Aplica segmentación y encolamiento (Lazy Load) para procesamiento masivo de imágenes fotográficas."""
        logger.info(f"Iniciando procesamiento de {len(image_paths)} imágenes sueltas. Aplicando segmentación.")
        
        try:
            sorted_images = sorted(image_paths, key=lambda p: (p.stat().st_mtime, p.name))
        except OSError as e:
            logger.warning(f"Imposible acceder a metadatos de sistema, ordenando alfabéticamente: {e}")
            sorted_images = sorted(image_paths, key=lambda p: p.name)

        batch_size = settings.vision_batch_size
        batches = [sorted_images[i:i + batch_size] for i in range(0, len(sorted_images), batch_size)]
        
        all_results: List[Dict[str, Any]] = []

        for batch_index, current_batch in enumerate(batches, 1):
            logger.info(f"Procesando fragmento fotográfico {batch_index}/{len(batches)} (Tamaño: {len(current_batch)} imágenes).")
            exploded_paths: List[Path] = []
            
            for img_path in current_batch:
                try:
                    exploded_paths.append(PDFToolbox.wrap_image_to_pdf(img_path, settings.explosion_dir))
                except OSError as e:
                    logger.error(f"Fallo de lectura de imagen {img_path.name}: {e}")
                except Exception as e:
                    logger.error(f"Error de conversión para imagen {img_path.name}: {e}", exc_info=True)

            if not exploded_paths:
                continue

            batch_id = f"LOTE_FOTOGRAFICO_G{batch_index:03d}"
            batch_results = self._execute_ai_pipeline(exploded_paths, batch_id, "IMÁGENES MIXTAS")
            all_results.extend(batch_results)

        return all_results

    def _execute_ai_pipeline(self, exploded_paths: List[Path], original_file_name: str, original_file_type: str) -> List[Dict[str, Any]]:
        batch_results: List[Dict[str, Any]] = []
        original_page_count = len(exploded_paths)
        exploded_paths.sort()
        path_map = {p.name: p for p in exploded_paths}

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
            
            try:
                category_dir = self._get_safe_category_dir(doc.document_type)
                category_dir.mkdir(exist_ok=True, parents=True)
            except ValueError as e:
                logger.error(f"Abortando ensamblaje por violación de seguridad: {e}")
                continue

            status = "OK" if doc.confidence_score > 80 else "REVISIÓN MANUAL"
            category_folder = category_dir.name
            
            for folio in doc.folios:
                ruta_segura_fallback = f"ERROR_ENSAMBLAJE/{folio}_{uuid.uuid4().hex[:8]}.pdf"
                try:
                    safe_folio = "".join(c for c in folio if c.isalnum() or c in ('-', '_')).strip()
                    if not safe_folio:
                        safe_folio = "ERROR_FORMATO_INVALIDO"

                    version, accion = self.db_repo.resolve_versioning(
                        safe_folio, doc.document_type, original_file_name, doc.confidence_score
                    )
                    
                    if accion == "DESCARTAR":
                        continue

                    file_name = f"{safe_folio}.pdf" if version == 1 else f"{safe_folio}_v{version}.pdf"
                    final_pdf_path = PDFToolbox.merge_by_folio(pages_data, file_name, category_dir)
                    ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                    
                    batch_results.append({
                        "Folio": folio,
                        "Versión": version,
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
                    batch_results.append({
                        "Folio": folio,
                        "Versión": 1,
                        "Categoría": doc.document_type,
                        "Cliente": doc.client_name or "NO DETECTADO",
                        "Archivo Original": original_file_name,
                        "Páginas Final": 0,
                        "Status": "ERROR_ENSAMBLAJE",
                        "Confianza": 0,
                        "Ruta del Archivo": ruta_segura_fallback,
                        "Justificación": f"Fallo sistema de archivos: {str(e)}"
                    })

        orphan_files = set(path_map.keys()) - used_pages
        if orphan_files:
            try:
                category_dir = self._get_safe_category_dir("Huerfanos_Rebotes")
                category_dir.mkdir(exist_ok=True, parents=True)
                category_folder = category_dir.name
            except ValueError:
                return batch_results
            
            for orphan in orphan_files:
                ruta_segura_huerfano = f"ERROR_ENSAMBLAJE/HUERFANO_{uuid.uuid4().hex[:8]}.pdf"
                try:
                    pages_data = [{"path": path_map[orphan], "rotation": 0}]
                    orphan_folio = f"HUERFANO_{Path(orphan).stem}"
                    file_name = f"{orphan_folio}.pdf"
                    final_pdf_path = PDFToolbox.merge_by_folio(pages_data, file_name, category_dir)
                    ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                    
                    batch_results.append({
                        "Folio": settings.error_ilegible,
                        "Versión": 1,
                        "Categoría": "Página Huérfana",
                        "Archivo Original": original_file_name,
                        "Páginas Final": 1,
                        "Status": "REVISIÓN MANUAL",
                        "Ruta del Archivo": ruta_relativa,
                        "Justificación": "Fragmento excluido por la inferencia."
                    })
                except Exception:
                    batch_results.append({
                        "Folio": settings.error_ilegible,
                        "Versión": 1,
                        "Categoría": "Página Huérfana",
                        "Archivo Original": original_file_name,
                        "Páginas Final": 0,
                        "Status": "ERROR_ENSAMBLAJE",
                        "Ruta del Archivo": ruta_segura_huerfano,
                        "Justificación": "Fallo físico al rescatar el fragmento."
                    })

        return batch_results