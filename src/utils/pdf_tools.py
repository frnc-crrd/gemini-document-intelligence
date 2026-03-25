"""Herramientas para manipulación física de archivos PDF e imágenes.

Provee métodos seguros para explotar documentos multipágina, aplicar
transformaciones espaciales permanentes (Physical Mutation) y reconstruir ensamblajes.
Implementa estrictamente manejadores de contexto para prevenir fugas de memoria
durante las interacciones con PyMuPDF (fitz).
"""

import fitz
import img2pdf
from pathlib import Path
from typing import List, Dict, Any

from src.core.logger import get_system_logger

logger = get_system_logger(__name__)


class PDFToolbox:
    """Clase utilitaria estática para operaciones de transformación de documentos."""

    @staticmethod
    def explode_pdf(input_path: Path, output_dir: Path) -> List[Path]:
        """Separa un PDF en archivos individuales de una sola página."""
        generated_files: List[Path] = []
        base_name = input_path.stem.replace(" ", "_")
        
        try:
            with fitz.open(str(input_path)) as doc:
                total_pages = len(doc)
                
                # Inyección de progreso físico
                logger.info(f"[I/O Físico] Explotando archivo '{input_path.name}' ({total_pages} páginas).")
                
                for i in range(total_pages):
                    page_num = i + 1
                    new_name = f"{base_name}_P{page_num:03d}_T{total_pages:03d}.pdf"
                    output_path = output_dir / new_name
                    
                    with fitz.open() as new_doc:
                        new_doc.insert_pdf(doc, from_page=i, to_page=i)
                        new_doc.save(str(output_path))
                        
                    generated_files.append(output_path)
                    
        except fitz.FileDataError as e:
            logger.error(f"Archivo PDF corrupto o ilegible {input_path}: {e}")
            raise RuntimeError(f"Fallo de lectura en archivo: {input_path}") from e
        except Exception as e:
            logger.error(f"Error inesperado al explotar PDF {input_path}: {e}", exc_info=True)
            raise RuntimeError("Fallo crítico en el motor de fragmentación PDF.") from e
            
        return generated_files

    @staticmethod
    def apply_physical_rotation(pdf_path: Path, degrees: int) -> None:
        """Aplica una rotación física permanente a un archivo PDF en disco.

        Garantiza que las siguientes fases operen sobre un artefacto nativamente alineado,
        eliminando la necesidad de transformaciones espaciales durante el reensamblaje.

        Args:
            pdf_path: Ruta al documento físico.
            degrees: Grados de rotación en sentido horario (0, 90, 180, 270).
        """
        if degrees == 0:
            return
            
        temp_path = pdf_path.with_suffix('.tmp.pdf')
        try:
            with fitz.open(str(pdf_path)) as doc:
                for page in doc:
                    page.set_rotation((page.rotation + degrees) % 360)
                doc.save(str(temp_path), incremental=False, encryption=fitz.PDF_ENCRYPT_NONE)
                
            temp_path.replace(pdf_path)
            logger.debug(f"Mutación física aplicada: {pdf_path.name} rotado {degrees} grados.")
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Fallo crítico al aplicar rotación física en {pdf_path}: {e}", exc_info=True)
            raise RuntimeError(f"Error de I/O al rotar PDF permanentemente: {e}") from e

    @staticmethod
    def wrap_image_to_pdf(image_path: Path, output_dir: Path) -> Path:
        """Convierte una imagen a un documento PDF de una sola página."""
        base_name = image_path.stem.replace(" ", "_")
        new_name = f"{base_name}_P001_T001.pdf"
        output_path = output_dir / new_name
        
        try:
            pdf_bytes = img2pdf.convert(str(image_path))
            with open(str(output_path), "wb") as f:
                f.write(pdf_bytes)
        except Exception as e:
            logger.error(f"Fallo al empaquetar imagen {image_path} a PDF: {e}", exc_info=True)
            raise
            
        return output_path

    @staticmethod
    def merge_by_folio(pages_data: List[Dict[str, Any]], file_name: str, output_dir: Path) -> Path:
        """Une múltiples PDFs secuencialmente utilizando el nombre exacto proporcionado.

        Acepta un diccionario con formato {'path': Path, 'rotation': int}. La rotación
        generalmente será 0 si la mutación física ya fue aplicada previamente.
        Permite sobrescribir el archivo destino si el orquestador determina deduplicación.

        Args:
            pages_data: Lista de diccionarios con la ruta y rotación de cada página.
            file_name: Nombre exacto del archivo final (e.g., 'FOLIO.pdf' o 'FOLIO_v2.pdf').
            output_dir: Directorio de destino.
        """
        output_path = output_dir / file_name
        
        # Eliminamos el archivo previo si existe para habilitar la Sobrescritura (Deduplicación de mejor calidad)
        if output_path.exists():
            output_path.unlink()
            logger.debug(f"Sobrescribiendo archivo existente para deduplicación: {file_name}")
        
        try:
            with fitz.open() as result_doc:
                for page_info in pages_data:
                    path: Path = page_info["path"]
                    rotation_to_apply: int = int(page_info.get("rotation", 0))
                    
                    with fitz.open(str(path)) as sub_doc:
                        if rotation_to_apply != 0:
                            for page in sub_doc:
                                current_rot = page.rotation
                                new_rot = (current_rot + rotation_to_apply) % 360
                                page.set_rotation(new_rot)
                        
                        result_doc.insert_pdf(sub_doc)
                
                result_doc.save(str(output_path))
        except Exception as e:
            logger.error(f"Error crítico al ensamblar documento {file_name}: {e}", exc_info=True)
            raise RuntimeError(f"Fallo en el ensamblaje final del documento {file_name}") from e
            
        return output_path