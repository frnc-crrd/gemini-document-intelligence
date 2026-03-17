"""Herramientas para manipulación física de archivos PDF e imágenes.

Provee métodos para explotar documentos multipágina en páginas individuales,
aplicar transformaciones espaciales y unificarlas.
"""

import fitz
import img2pdf
from pathlib import Path
from typing import List, Dict, Any

class PDFToolbox:
    """Clase utilitaria para operaciones de transformación de documentos."""

    @staticmethod
    def explode_pdf(input_path: Path, output_dir: Path) -> List[Path]:
        """Separa un PDF en archivos de una sola página."""
        doc = fitz.open(str(input_path))
        total_pages = len(doc)
        generated_files = []
        
        base_name = input_path.stem.replace(" ", "_")
        
        for i in range(total_pages):
            page_num = i + 1
            new_name = f"{base_name}_P{page_num:03}_T{total_pages:03}.pdf"
            output_path = output_dir / new_name
            
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=i, to_page=i)
            new_doc.save(str(output_path))
            new_doc.close()
            generated_files.append(output_path)
            
        doc.close()
        return generated_files

    @staticmethod
    def wrap_image_to_pdf(image_path: Path, output_dir: Path) -> Path:
        """Convierte una imagen a un PDF de una sola página sin pérdida de calidad."""
        base_name = image_path.stem.replace(" ", "_")
        new_name = f"{base_name}_P001_T001.pdf"
        output_path = output_dir / new_name
        
        with open(str(output_path), "wb") as f:
            f.write(img2pdf.convert(str(image_path)))
            
        return output_path

    @staticmethod
    def merge_by_folio(pages_data: List[Dict[str, Any]], folio: str, output_dir: Path) -> Path:
        """Une múltiples PDFs aplicando rotaciones específicas por página.

        Args:
            pages_data: Lista de diccionarios [{'path': Path, 'rotation': int}].
                        El orden de esta lista dicta el orden en el documento final.
            folio: El identificador que dará nombre al archivo final.
            output_dir: Directorio de salida.

        Returns:
            Ruta al archivo PDF final unificado.
        """
        safe_folio = "".join(c for c in folio if c.isalnum() or c in ('-', '_')).strip()
        if not safe_folio:
            safe_folio = "ERROR_FORMATO_INVALIDO"
            
        output_path = output_dir / f"{safe_folio}.pdf"
        
        counter = 1
        while output_path.exists():
            output_path = output_dir / f"{safe_folio}_V{counter}.pdf"
            counter += 1
        
        result_doc = fitz.open()
        
        for page_info in pages_data:
            path = page_info["path"]
            rotation_to_apply = page_info.get("rotation", 0)
            
            sub_doc = fitz.open(str(path))
            
            # Aplicar rotación visual si Gemini lo solicitó
            if rotation_to_apply != 0:
                for page in sub_doc:
                    current_rot = page.rotation
                    # Sumamos la rotación actual para no sobreescribir metadatos previos
                    new_rot = (current_rot + rotation_to_apply) % 360
                    page.set_rotation(new_rot)
            
            result_doc.insert_pdf(sub_doc)
            sub_doc.close()
            
        result_doc.save(str(output_path))
        result_doc.close()
        return output_path