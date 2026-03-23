"""Orquestador principal del pipeline de integridad documental.

Coordina la ingesta, explosión, análisis contextual paralelo y ensamblaje final.
Implementa un patrón Map-Reduce estricto para resolver la fragmentación lógica
de documentos distribuidos en múltiples lotes de visión computacional, garantizando
la consolidación física antes de la persistencia transaccional.
"""

import re
import uuid
from pathlib import Path
from typing import List, Dict, Any, Set, Optional, Protocol, Tuple
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
    """Controlador central del flujo de trabajo documental basado en Map-Reduce."""

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
        if not raw_files:
            return []

        logger.info(f"Iniciando procesamiento masivo de {len(raw_files)} archivo(s) físico(s) bajo arquitectura Map-Reduce.")

        pdf_files = [f for f in raw_files if f.suffix.lower() == '.pdf']
        image_files = [f for f in raw_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png']]

        mapped_items: List[Dict[str, Any]] = []

        # MAP: Fase de Inferencia Paralela para PDFs autocontenidos
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pdf = {executor.submit(self._map_single_pdf, pdf): pdf for pdf in pdf_files}
            for future in as_completed(future_to_pdf):
                pdf_path = future_to_pdf[future]
                try:
                    mapped_items.extend(future.result())
                except Exception as e:
                    logger.error(f"Fallo catastrófico en el hilo procesando {pdf_path.name}: {e}", exc_info=True)

        # MAP: Fase de Inferencia Secuencial y Segmentada para Imágenes Sueltas
        if image_files:
            try:
                mapped_items.extend(self._map_loose_images(image_files))
            except Exception as e:
                logger.error(f"Fallo al mapear lote de imágenes: {e}", exc_info=True)

        # REDUCE: Agrupación y Ensamblaje Físico Consolidado
        final_results = self._shuffle_and_reduce(mapped_items)
        
        logger.info(f"Procesamiento Map-Reduce completado. Total de entidades lógicas generadas: {len(final_results)}.")
        return final_results

    def _map_single_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        logger.info(f"Iniciando explosión y mapeo de PDF: {file_path.name}")
        try:
            exploded_paths = PDFToolbox.explode_pdf(file_path, settings.explosion_dir)
            if not exploded_paths: 
                return []
            return self._map_ai_pipeline(exploded_paths, file_path.name, "PDF")
        except fitz.FileDataError as e:
            logger.error(f"Estructura de PDF corrupta o ilegible al procesar {file_path.name}: {e}")
            return []
        except OSError as e:
            logger.error(f"Error de sistema operativo (I/O) al acceder a {file_path.name}: {e}")
            return []

    def _map_loose_images(self, image_paths: List[Path]) -> List[Dict[str, Any]]:
        logger.info(f"Iniciando mapeo de {len(image_paths)} imágenes sueltas. Aplicando segmentación por lotes.")
        
        try:
            sorted_images = sorted(image_paths, key=lambda p: (p.stat().st_mtime, p.name))
        except OSError as e:
            logger.warning(f"Imposible acceder a metadatos de sistema, ordenando alfabéticamente: {e}")
            sorted_images = sorted(image_paths, key=lambda p: p.name)

        batch_size = settings.vision_batch_size
        batches = [sorted_images[i:i + batch_size] for i in range(0, len(sorted_images), batch_size)]
        
        mapped_items: List[Dict[str, Any]] = []

        for batch_index, current_batch in enumerate(batches, 1):
            logger.info(f"Mapeando fragmento fotográfico {batch_index}/{len(batches)} (Tamaño: {len(current_batch)} imágenes).")
            exploded_paths: List[Path] = []
            
            for img_path in current_batch:
                try:
                    exploded_paths.append(PDFToolbox.wrap_image_to_pdf(img_path, settings.explosion_dir))
                except OSError as e:
                    logger.error(f"Fallo de lectura de imagen {img_path.name}: {e}")
                except Exception as e:
                    logger.error(f"Error de conversión para imagen {img_path.name}: {e}", exc_info=True)

            if not exploded_paths: continue

            # VITAL: Se unifica el origen para permitir que la fase Shuffle agrupe documentos entre lotes distintos
            batch_id = "LOTE_FOTOGRAFICO_CONSOLIDADO"
            mapped_items.extend(self._map_ai_pipeline(exploded_paths, batch_id, "IMÁGENES MIXTAS"))

        return mapped_items

    def _map_ai_pipeline(self, exploded_paths: List[Path], original_file_name: str, original_file_type: str) -> List[Dict[str, Any]]:
        """Extrae intenciones de ensamblaje desde el motor LLM sin persistir archivos en disco."""
        items = []
        original_page_count = len(exploded_paths)
        exploded_paths.sort()
        path_map = {p.name: p for p in exploded_paths}

        analysis_result = self.analyzer.analyze_batch(exploded_paths, original_file_name)
        used_pages: Set[str] = set()

        for doc in analysis_result.documents:
            pages_data = []
            for page_instruction in doc.pages:
                clean_filename = page_instruction.file_name.strip().strip("'\"")
                if clean_filename in path_map:
                    pages_data.append({
                        "path": path_map[clean_filename],
                        "rotation": page_instruction.rotation_degrees
                    })
                    used_pages.add(clean_filename)

            if not pages_data: continue

            for folio in doc.folios:
                items.append({
                    "folio": folio,
                    "categoria": doc.document_type,
                    "cliente": doc.client_name,
                    "confianza": doc.confidence_score,
                    "justificacion": doc.reasoning,
                    "origen": original_file_name,
                    "tipo_origen": original_file_type,
                    "paginas": pages_data,
                    "is_orphan": False,
                    "original_page_count": original_page_count
                })

        orphan_files = set(path_map.keys()) - used_pages
        for orphan in orphan_files:
            orphan_folio = f"HUERFANO_{Path(orphan).stem}"
            items.append({
                "folio": settings.error_ilegible,
                "categoria": "Página Huérfana",
                "cliente": "NO DETECTADO",
                "confianza": 0,
                "justificacion": "Fragmento excluido por la inferencia.",
                "origen": original_file_name,
                "tipo_origen": original_file_type,
                "paginas": [{"path": path_map[orphan], "rotation": 0}],
                "is_orphan": True,
                "orphan_name": orphan_folio,
                "original_page_count": original_page_count
            })

        return items

    def _shuffle_and_reduce(self, mapped_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Agrupa intenciones de ensamblaje por folio/categoría y materializa los PDFs finales."""
        logger.info("Iniciando Fase de Shuffle & Reduce: Agrupando documentos fragmentados.")
        groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        orphans = []
        
        # FASE SHUFFLE: Agrupamiento estructurado
        for item in mapped_items:
            if item["is_orphan"]:
                orphans.append(item)
                continue
                
            safe_folio = "".join(c for c in item["folio"] if c.isalnum() or c in ('-', '_')).strip()
            if not safe_folio: safe_folio = "ERROR_FORMATO_INVALIDO"
            
            key = (safe_folio, item["categoria"], item["origen"])
            if key not in groups: groups[key] = []
            groups[key].append(item)
            
        final_results = []
        
        # FASE REDUCE: Materialización física e inyección de base de datos
        for (safe_folio, categoria, origen), items in groups.items():
            all_pages = []
            seen_paths = set()
            
            # Combina páginas respetando el orden cronológico original de los lotes
            for i in items:
                for p in i["paginas"]:
                    if p["path"] not in seen_paths:
                        seen_paths.add(p["path"])
                        all_pages.append(p)
                        
            clientes = [i["cliente"] for i in items if i["cliente"] and i["cliente"] != "NO DETECTADO"]
            cliente_final = clientes[0] if clientes else "NO DETECTADO"
            
            confianza_final = max(i["confianza"] for i in items)
            justificaciones = list(dict.fromkeys(i["justificacion"] for i in items))
            justificacion_final = " | ".join(justificaciones)
            
            tipo_origen = items[0]["tipo_origen"]
            original_page_count = sum(i["original_page_count"] for i in items) 
            folio_text = items[0]["folio"]
            
            try:
                category_dir = self._get_safe_category_dir(categoria)
                category_dir.mkdir(exist_ok=True, parents=True)
            except ValueError as e:
                logger.error(f"Abortando ensamblaje por violación de seguridad: {e}")
                continue
                
            status = "OK" if confianza_final > 80 else "REVISIÓN MANUAL"
            category_folder = category_dir.name
            ruta_segura_fallback = f"ERROR_ENSAMBLAJE/{safe_folio}_{uuid.uuid4().hex[:8]}.pdf"
            
            try:
                version, accion = self.db_repo.resolve_versioning(
                    safe_folio, categoria, origen, confianza_final
                )
                
                if accion == "DESCARTAR": continue
                
                file_name = f"{safe_folio}.pdf" if version == 1 else f"{safe_folio}_v{version}.pdf"
                final_pdf_path = PDFToolbox.merge_by_folio(all_pages, file_name, category_dir)
                
                final_results.append({
                    "Folio": folio_text,
                    "Versión": version,
                    "Categoría": categoria,
                    "Cliente": cliente_final,
                    "Archivo Original": origen,
                    "Tipo Original": tipo_origen,
                    "Páginas Original": original_page_count,
                    "Páginas Final": len(all_pages),
                    "Status": status,
                    "Confianza": confianza_final,
                    "Ruta del Archivo": f"{category_folder}/{final_pdf_path.name}",
                    "Justificación": justificacion_final
                })
            except Exception as e:
                logger.error(f"Fallo al ensamblar documento final para folio {safe_folio}: {e}", exc_info=True)
                final_results.append({
                    "Folio": folio_text,
                    "Versión": 1,
                    "Categoría": categoria,
                    "Cliente": cliente_final,
                    "Archivo Original": origen,
                    "Páginas Final": 0,
                    "Status": "ERROR_ENSAMBLAJE",
                    "Confianza": 0,
                    "Ruta del Archivo": ruta_segura_fallback,
                    "Justificación": f"Fallo sistema de archivos: {str(e)}"
                })
                
        # Procesamiento aislado para huérfanos (Asegurando conservación de masa)
        if orphans:
            try:
                orphan_dir = self._get_safe_category_dir("Huerfanos_Rebotes")
                orphan_dir.mkdir(exist_ok=True, parents=True)
                orphan_folder = orphan_dir.name
            except ValueError:
                return final_results
                
            for orphan in orphans:
                orphan_name = orphan["orphan_name"]
                file_name = f"{orphan_name}.pdf"
                ruta_segura_huerfano = f"ERROR_ENSAMBLAJE/{orphan_name}_{uuid.uuid4().hex[:8]}.pdf"
                
                try:
                    final_pdf_path = PDFToolbox.merge_by_folio(orphan["paginas"], file_name, orphan_dir)
                    final_results.append({
                        "Folio": settings.error_ilegible,
                        "Versión": 1,
                        "Categoría": orphan["categoria"],
                        "Archivo Original": orphan["origen"],
                        "Páginas Final": 1,
                        "Status": "REVISIÓN MANUAL",
                        "Ruta del Archivo": f"{orphan_folder}/{final_pdf_path.name}",
                        "Justificación": orphan["justificacion"]
                    })
                except Exception as e:
                    final_results.append({
                        "Folio": settings.error_ilegible,
                        "Versión": 1,
                        "Categoría": orphan["categoria"],
                        "Archivo Original": orphan["origen"],
                        "Páginas Final": 0,
                        "Status": "ERROR_ENSAMBLAJE",
                        "Ruta del Archivo": ruta_segura_huerfano,
                        "Justificación": f"Fallo físico al rescatar fragmento: {e}"
                    })
                    
        return final_results