"""Herramientas para manipulación física de archivos PDF e imágenes.

Provee métodos seguros para explotar documentos multipágina, aplicar
transformaciones espaciales y reconstruir ensamblajes. Implementa
estrictamente manejadores de contexto para prevenir fugas de memoria
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
        """Separa un PDF en archivos individuales de una sola página.

        Args:
            input_path: Ruta al archivo PDF físico origen.
            output_dir: Directorio destino para las páginas generadas.

        Returns:
            List[Path]: Lista de rutas físicas de las páginas extraídas.
            
        Raises:
            RuntimeError: Si ocurre un fallo a nivel del motor PDF.
        """
        generated_files: List[Path] = []
        base_name = input_path.stem.replace(" ", "_")
        
        try:
            with fitz.open(str(input_path)) as doc:
                total_pages = len(doc)
                
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
    def wrap_image_to_pdf(image_path: Path, output_dir: Path) -> Path:
        """Convierte una imagen a un documento PDF de una sola página.

        Args:
            image_path: Ruta a la imagen original (JPG, PNG).
            output_dir: Directorio de destino.

        Returns:
            Path: Ruta del nuevo archivo PDF generado.
        """
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
    def merge_by_folio(pages_data: List[Dict[str, Any]], folio: str, output_dir: Path) -> Path:
        """Une múltiples PDFs aplicando rotaciones específicas por página.

        Garantiza que las páginas se compilen en el orden proveído y ajusta
        la orientación espacial sumando los grados indicados a los metadatos
        existentes del documento.

        Args:
            pages_data: Diccionarios con formato {'path': Path, 'rotation': int}.
            folio: Identificador lógico para nombrar el archivo final.
            output_dir: Directorio de salida.

        Returns:
            Path: Ruta al archivo PDF consolidado.
        """
        safe_folio = "".join(c for c in folio if c.isalnum() or c in ('-', '_')).strip()
        if not safe_folio:
            safe_folio = "ERROR_FORMATO_INVALIDO"
            
        output_path = output_dir / f"{safe_folio}.pdf"
        
        counter = 1
        while output_path.exists():
            output_path = output_dir / f"{safe_folio}_V{counter}.pdf"
            counter += 1
        
        try:
            with fitz.open() as result_doc:
                for page_info in pages_data:
                    path: Path = page_info["path"]
                    rotation_to_apply: int = page_info.get("rotation", 0)
                    
                    with fitz.open(str(path)) as sub_doc:
                        if rotation_to_apply != 0:
                            for page in sub_doc:
                                current_rot = page.rotation
                                new_rot = (current_rot + rotation_to_apply) % 360
                                page.set_rotation(new_rot)
                        
                        result_doc.insert_pdf(sub_doc)
                
                result_doc.save(str(output_path))
        except Exception as e:
            logger.error(f"Error crítico al ensamblar folio {folio}: {e}", exc_info=True)
            raise RuntimeError(f"Fallo en el ensamblaje final del documento {folio}") from e
            
        return output_path