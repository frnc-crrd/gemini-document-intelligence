"""Orquestador principal del pipeline de integridad documental.

Coordina la lectura, explosión, análisis contextual y la reconstrucción.
Implementa el "Recogedor de Huérfanos" para garantizar la conservación de masa
(ninguna página original puede quedar fuera del reporte final).
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Any

from src import config
from src.core.analyzer import DocumentAnalyzer
from src.utils.pdf_tools import PDFToolbox


class PipelineProcessor:
    def __init__(self):
        self._ensure_directories()
        self.analyzer = DocumentAnalyzer()

    def _ensure_directories(self) -> None:
        for directory in [config.RAW_DIR, config.EXPLOSION_DIR, config.FINAL_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

    def _sanitize_folder_name(self, name: str) -> str:
        s = re.sub(r'[^\w\s-]', '', name).strip()
        return re.sub(r'[\s]+', '_', s)

    def run(self) -> List[Dict[str, Any]]:
        raw_files = [f for f in config.RAW_DIR.iterdir() if f.is_file()]
        all_results = []
        
        if not raw_files:
            print(f"No hay archivos para procesar en: {config.RAW_DIR}")
            return all_results

        print(f"Iniciando procesamiento...\n")

        pdf_files = [f for f in raw_files if f.suffix.lower() == '.pdf']
        image_files = [f for f in raw_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png']]

        for pdf in pdf_files:
            file_results = self._process_single_pdf(pdf)
            all_results.extend(file_results)

        if image_files:
            file_results = self._process_loose_images(image_files)
            all_results.extend(file_results)

        print("\nProcesamiento masivo completado.")
        return all_results

    def _process_single_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        print(f"--- Procesando PDF: {file_path.name} ---")
        exploded_paths = PDFToolbox.explode_pdf(file_path, config.EXPLOSION_DIR)
        if not exploded_paths: return []
        
        return self._execute_ai_pipeline(
            exploded_paths=exploded_paths, 
            original_file_name=file_path.name, 
            original_file_type="PDF"
        )

    def _process_loose_images(self, image_paths: List[Path]) -> List[Dict[str, Any]]:
        print(f"--- Procesando Lote Masivo de {len(image_paths)} Imágenes Sueltas ---")
        exploded_paths = []
        
        for p in image_paths:
            wrapped = PDFToolbox.wrap_image_to_pdf(p, config.EXPLOSION_DIR)
            exploded_paths.append(wrapped)
            
        if not exploded_paths: return []
        
        return self._execute_ai_pipeline(
            exploded_paths=exploded_paths, 
            original_file_name="LOTE_FOTOGRAFICO_SUELTO", 
            original_file_type="IMÁGENES MIXTAS"
        )

    def _execute_ai_pipeline(self, exploded_paths: List[Path], original_file_name: str, original_file_type: str) -> List[Dict[str, Any]]:
        batch_results = []
        original_page_count = len(exploded_paths)
        exploded_paths.sort()
        path_map = {p.name: p for p in exploded_paths}

        print(f"  Analizando estructura y rotación de {original_page_count} página(s)...")
        analysis_result = self.analyzer.analyze_batch(exploded_paths, original_file_name)

        # Set para rastrear qué páginas usó realmente la IA
        used_pages = set()

        print(f"  Ensamblando y categorizando documentos físicos...")
        for doc in analysis_result.documents:
            pages_data = []
            for page_instruction in doc.pages:
                clean_filename = page_instruction.file_name.strip().strip("'\"")
                if clean_filename in path_map:
                    pages_data.append({
                        "path": path_map[clean_filename],
                        "rotation": page_instruction.rotation_degrees
                    })
                    used_pages.add(clean_filename) # Marcamos la página como utilizada

            if not pages_data:
                continue

            final_page_count = len(pages_data)
            category_folder = self._sanitize_folder_name(doc.document_type)
            category_dir = config.FINAL_DIR / category_folder
            category_dir.mkdir(exist_ok=True)

            status = "OK" if doc.confidence_score > 80 else "REVISIÓN MANUAL"
            
            for folio in doc.folios:
                final_pdf_path = PDFToolbox.merge_by_folio(
                    pages_data=pages_data,
                    folio=folio,
                    output_dir=category_dir
                )
                
                ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                print(f"    -> Guardado: {ruta_relativa}")
                
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

        # ==========================================
        # RECOGEDOR DE HUÉRFANOS (CONSERVACIÓN DE MASA)
        # ==========================================
        orphan_files = set(path_map.keys()) - used_pages
        if orphan_files:
            print(f"    [!] ALERTA: La IA omitió {len(orphan_files)} página(s). Rescatando huérfanos...")
            category_folder = "Huerfanos_Rebotes"
            category_dir = config.FINAL_DIR / category_folder
            category_dir.mkdir(exist_ok=True)
            
            for orphan in orphan_files:
                # Rescatar la página con rotación 0 por defecto
                pages_data = [{"path": path_map[orphan], "rotation": 0}]
                final_pdf_path = PDFToolbox.merge_by_folio(
                    pages_data=pages_data,
                    folio=f"HUERFANO_{Path(orphan).stem}",
                    output_dir=category_dir
                )
                
                ruta_relativa = f"{category_folder}/{final_pdf_path.name}"
                print(f"    -> Rescatado: {ruta_relativa}")
                
                batch_results.append({
                    "Folio": "SIN_FOLIO",
                    "Categoría": "Página Huérfana",
                    "Cliente": "NO DETECTADO",
                    "Archivo Original": original_file_name,
                    "Tipo Original": original_file_type,
                    "Páginas Original": original_page_count,
                    "Páginas Final": 1,
                    "Status": "REVISIÓN MANUAL",
                    "Confianza": 0,
                    "Ruta del Archivo": ruta_relativa,
                    "Justificación": "La IA omitió esta página. Fue rescatada automáticamente por el sistema para evitar pérdida de información."
                })

        return batch_results