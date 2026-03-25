"""Orquestador principal del pipeline de integridad documental.

Coordina la ingesta, explosión, análisis contextual paralelo y ensamblaje final.
Implementa el enrutamiento Multiplex (Dual-Save) para documentos de categoría combinada
y envía fragmentos sin identificar a la carpeta Revision_Manual_Requerida.
Garantiza el manejo del estado mediante la correcta disposición de Yields transaccionales.
"""

import re
import uuid
import shutil
from pathlib import Path
from typing import List, Dict, Any, Set, Optional, Protocol, Tuple, Generator
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
    """Controlador central del flujo de trabajo documental basado en Map-Reduce iterativo."""

    def __init__(self, analyzer: IDocumentAnalyzer, db_repo: Optional[PostgresRepository] = None) -> None:
        self._ensure_directories()
        self.analyzer = analyzer
        self.db_repo = db_repo or PostgresRepository()
        self.max_workers = settings.max_threads

    def _ensure_directories(self) -> None:
        for directory in [settings.raw_dir, settings.explosion_dir, settings.final_dir, settings.processed_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def _limpiar_basura_previa(self) -> None:
        """Purga archivos de sistema locales que no aportan valor algorítmico."""
        extensiones_basura = {".db", ".ini", ".ds_store"}
        for archivo in settings.raw_dir.iterdir():
            if archivo.is_file() and (archivo.suffix.lower() in extensiones_basura or archivo.name.startswith(".")):
                try:
                    archivo.unlink()
                    logger.debug(f"Archivo basura eliminado de la ingesta: {archivo.name}")
                except OSError as e:
                    logger.warning(f"Imposible purgar archivo basura {archivo.name}: {e}")

    def _sanitize_folder_name(self, name: str) -> str:
        s = re.sub(r'[^\w\s-]', '', name).strip()
        return re.sub(r'[\s]+', '_', s)

    def _get_safe_category_dir(self, category_name: str) -> Path:
        """Resuelve y valida la ruta de destino mitigando vulnerabilidades de Path Traversal."""
        # Rechazo explícito previo a la sanitización
        if ".." in category_name or "/" in category_name or "\\" in category_name:
            logger.critical(f"Intento de Path Traversal detectado y bloqueado para categoría: {category_name}")
            raise ValueError(f"Ruta de destino contiene caracteres de navegación no permitidos: {category_name}")

        category_folder = self._sanitize_folder_name(category_name)
        category_dir = (settings.final_dir / category_folder).resolve()
        
        if not category_dir.is_relative_to(settings.final_dir.resolve()):
            logger.critical(f"Validación de frontera fallida para categoría: {category_name}")
            raise ValueError(f"Ruta de destino inválida o maliciosa: {category_dir}")
            
        return category_dir

    def _mover_a_procesados(self, archivos: List[Path]) -> None:
        """Traslada físicamente los archivos originales una vez persistidos."""
        for f_path in archivos:
            if f_path.exists():
                dest_path = settings.processed_dir / f_path.name
                try:
                    shutil.move(str(f_path), str(dest_path))
                except Exception as e:
                    logger.error(f"Fallo al mover documento a procesados {f_path.name}: {e}")

    def _agrupar_pdfs_por_masa(self, pdf_paths: List[Path], max_pages: int) -> List[List[Path]]:
        """Algoritmo Greedy Bin Packing para agrupar PDFs garantizando lotes balanceados en memoria."""
        chunks: List[List[Path]] = []
        current_chunk: List[Path] = []
        current_pages = 0

        for path in pdf_paths:
            try:
                with fitz.open(str(path)) as doc:
                    paginas_pdf = len(doc)
            except Exception as e:
                logger.error(f"Descartando archivo corrupto de la cola de procesamiento {path.name}: {e}")
                continue

            if current_chunk and (current_pages + paginas_pdf > max_pages):
                chunks.append(current_chunk)
                current_chunk = []
                current_pages = 0

            current_chunk.append(path)
            current_pages += paginas_pdf

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def _agrupar_imagenes_por_lotes_seguros(self, image_paths: List[Path], max_size: int) -> List[List[Path]]:
        """Aplica una heurística basada en tiempo para evitar dividir un documento multipágina entre lotes."""
        try:
            sorted_imgs = sorted(image_paths, key=lambda p: (p.stat().st_mtime, p.name))
        except OSError:
            sorted_imgs = sorted(image_paths, key=lambda p: p.name)

        chunks: List[List[Path]] = []
        current_chunk: List[Path] = []

        for i, img in enumerate(sorted_imgs):
            current_chunk.append(img)
            
            if len(current_chunk) >= max_size:
                if i + 1 < len(sorted_imgs):
                    try:
                        time_diff = abs(sorted_imgs[i+1].stat().st_mtime - img.stat().st_mtime)
                        if time_diff < 3.0:
                            continue
                    except OSError:
                        pass
                
                chunks.append(current_chunk)
                current_chunk = []
        
        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def run(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Ejecuta el procesamiento con enrutamiento inteligente y balanceo de carga."""
        self._limpiar_basura_previa()
        
        raw_files = sorted([f for f in settings.raw_dir.iterdir() if f.is_file()])
        if not raw_files:
            return

        pdf_files = [f for f in raw_files if f.suffix.lower() == '.pdf']
        image_files = [f for f in raw_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png']]

        if pdf_files:
            logger.info(f"Evaluando topología física de {len(pdf_files)} PDFs para empaquetado dinámico...")
            pdf_chunks = self._agrupar_pdfs_por_masa(pdf_files, settings.pdf_chunk_max_pages)
            
            for idx, chunk in enumerate(pdf_chunks, 1):
                logger.info(f"--- PROCESANDO LOTE DE PDFs {idx}/{len(pdf_chunks)} ({len(chunk)} archivos balanceados) ---")
                mapped_items: List[Dict[str, Any]] = []
                abort_system = False
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_pdf = {executor.submit(self._map_single_pdf, pdf): pdf for pdf in chunk}
                    for future in as_completed(future_to_pdf):
                        pdf_path = future_to_pdf[future]
                        try:
                            mapped_items.extend(future.result())
                        except ConnectionAbortedError:
                            logger.critical(f"Cortacircuitos activado procesando {pdf_path.name}. Abortando hilo.")
                            abort_system = True
                        except Exception as e:
                            logger.error(f"Fallo catastrófico en el hilo procesando {pdf_path.name}: {e}", exc_info=True)
                
                if abort_system:
                    logger.critical("Interrumpiendo orquestador debido a bloqueo de proveedor de API (Hard Cap 429).")
                    return

                final_results = self._shuffle_and_reduce(mapped_items)
                
                # Inyección Yield Controlada: Previene mover archivos si la transacción superior falla
                if final_results:
                    yield final_results
                
                self._mover_a_procesados(chunk)

        if image_files:
            logger.info(f"Evaluando topología temporal de {len(image_files)} imágenes para empaquetado seguro...")
            image_chunks = self._agrupar_imagenes_por_lotes_seguros(image_files, settings.physical_chunk_size)
            
            for idx, chunk in enumerate(image_chunks, 1):
                logger.info(f"--- PROCESANDO LOTE DE IMÁGENES {idx}/{len(image_chunks)} ({len(chunk)} archivos aglomerados temporalmente) ---")
                try:
                    mapped_items = self._map_loose_images(chunk)
                    final_results = self._shuffle_and_reduce(mapped_items)
                    
                    if final_results:
                        yield final_results
                        
                    self._mover_a_procesados(chunk)
                except ConnectionAbortedError:
                    logger.critical("Cortacircuitos activado en flujo fotográfico (Hard Cap 429). Interrumpiendo orquestador para prevenir pérdida de datos.")
                    return
                except Exception as e:
                    logger.error(f"Fallo al procesar lote fotográfico {idx}: {e}", exc_info=True)

    def _map_single_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        try:
            exploded_paths = PDFToolbox.explode_pdf(file_path, settings.explosion_dir)
            if not exploded_paths: 
                return []
            items = self._map_ai_pipeline(exploded_paths, file_path.name, "PDF")
            logger.info(f"[Mapeo] Finalizado para '{file_path.name}': {len(items)} fragmentos extraídos hacia el orquestador.")
            return items
        except ConnectionAbortedError:
            raise
        except fitz.FileDataError as e:
            logger.error(f"Estructura de PDF corrupta o ilegible al procesar {file_path.name}: {e}")
            return []
        except OSError as e:
            logger.error(f"Error de sistema operativo (I/O) al acceder a {file_path.name}: {e}")
            return []

    def _map_loose_images(self, chunk_paths: List[Path]) -> List[Dict[str, Any]]:
        batch_size = settings.vision_batch_size
        batches = [chunk_paths[i:i + batch_size] for i in range(0, len(chunk_paths), batch_size)]
        
        mapped_items: List[Dict[str, Any]] = []

        for batch_index, current_batch in enumerate(batches, 1):
            logger.info(f"[Map Fotográfico] Preparando ráfaga API {batch_index}/{len(batches)} (Tamaño: {len(current_batch)} imágenes).")
            exploded_paths: List[Path] = []
            
            for img_path in current_batch:
                try:
                    exploded_paths.append(PDFToolbox.wrap_image_to_pdf(img_path, settings.explosion_dir))
                except OSError as e:
                    logger.error(f"Fallo de lectura de imagen {img_path.name}: {e}")
                except Exception as e:
                    logger.error(f"Error de conversión para imagen {img_path.name}: {e}", exc_info=True)

            if not exploded_paths: continue

            batch_id = "LOTE_FOTOGRAFICO_CONSOLIDADO"
            items = self._map_ai_pipeline(exploded_paths, batch_id, "IMÁGENES MIXTAS")
            mapped_items.extend(items)

        return mapped_items

    def _map_ai_pipeline(self, exploded_paths: List[Path], original_file_name: str, original_file_type: str) -> List[Dict[str, Any]]:
        items = []
        original_page_count = len(exploded_paths)
        
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
        logger.info(f"[Reduce] Iniciando consolidación física y heurística de {len(mapped_items)} fragmentos.")
        groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        orphans = []
        
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
        
        for (safe_folio, categoria_cruda, origen), items in groups.items():
            all_pages = []
            seen_paths = set()
            
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
            original_page_count = items[0]["original_page_count"] if items else 0
            folio_text = items[0]["folio"]
            status = "OK" if confianza_final > 80 else "REVISIÓN MANUAL"
            
            categorias_destino = [c.strip() for c in categoria_cruda.split(',') if c.strip()]
            if not categorias_destino:
                categorias_destino = ["No_identificado"]
                
            for cat_individual in categorias_destino:
                # Asignación segura temprana para evitar UnboundLocalError y proveer entropía única
                ruta_segura_fallback = f"ERROR_ENSAMBLAJE/{safe_folio}_{uuid.uuid4().hex[:8]}.pdf"
                
                try:
                    category_dir = self._get_safe_category_dir(cat_individual)
                    category_dir.mkdir(exist_ok=True, parents=True)
                    category_folder = category_dir.name
                    
                    version, accion = self.db_repo.resolve_versioning(
                        safe_folio, cat_individual, origen, confianza_final
                    )
                    
                    if accion == "DESCARTAR": continue
                    
                    file_name = f"{safe_folio}.pdf" if version == 1 else f"{safe_folio}_v{version}.pdf"
                    final_pdf_path = PDFToolbox.merge_by_folio(all_pages, file_name, category_dir)
                    
                    logger.info(f"[Reduce] Ensamblaje exitoso ({cat_individual}): '{file_name}'.")
                    
                    final_results.append({
                        "Folio": folio_text,
                        "Versión": version,
                        "Categoría": cat_individual,
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
                    logger.error(f"Fallo al ensamblar documento {cat_individual} para folio {safe_folio}: {e}", exc_info=True)
                    final_results.append({
                        "Folio": folio_text,
                        "Versión": 1,
                        "Categoría": cat_individual,
                        "Cliente": cliente_final,
                        "Archivo Original": origen,
                        "Páginas Final": 0,
                        "Status": "ERROR_ENSAMBLAJE",
                        "Confianza": 0,
                        "Ruta del Archivo": ruta_segura_fallback,
                        "Justificación": f"Fallo sistema de archivos: {str(e)}"
                    })
                
        if orphans:
            try:
                orphan_dir = self._get_safe_category_dir("Revision_Manual_Requerida")
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
                        "Categoría": "Revisión Manual Requerida",
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
                        "Categoría": "Revisión Manual Requerida",
                        "Archivo Original": orphan["origen"],
                        "Páginas Final": 0,
                        "Status": "ERROR_ENSAMBLAJE",
                        "Ruta del Archivo": ruta_segura_huerfano,
                        "Justificación": f"Fallo físico al rescatar fragmento: {e}"
                    })
                    
        return final_results