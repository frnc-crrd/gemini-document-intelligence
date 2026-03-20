"""Pruebas unitarias para utilidades físicas de manipulación de PDF.

Se utilizan mocks sobre la librería PyMuPDF (fitz) para evitar la creación
de archivos físicos innecesarios en el disco durante la ejecución de los tests.
Valida la nueva heurística de mutación física permanente (OCR-Aware) y 
la lógica de desvinculación (unlink) para deduplicación.
"""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from src.utils.pdf_tools import PDFToolbox


@patch("src.utils.pdf_tools.fitz.open")
def test_explode_pdf_success(mock_fitz_open: MagicMock, tmp_path: Path) -> None:
    """Valida la correcta iteración y separación de páginas en archivos individuales."""
    mock_doc = MagicMock()
    mock_doc.__len__.return_value = 2  # Simular PDF de 2 páginas
    
    mock_fitz_open.return_value.__enter__.return_value = mock_doc

    input_pdf = tmp_path / "test_doc.pdf"
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    result = PDFToolbox.explode_pdf(input_pdf, output_dir)

    assert len(result) == 2
    assert "test_doc_P001_T002.pdf" in result[0].name
    assert "test_doc_P002_T002.pdf" in result[1].name


@patch("src.utils.pdf_tools.fitz.open")
def test_apply_physical_rotation_success(mock_fitz_open: MagicMock, tmp_path: Path) -> None:
    """Asegura que la mutación física altere el objeto de página y reemplace el original en disco."""
    mock_doc = MagicMock()
    mock_page = MagicMock()
    mock_page.rotation = 0
    mock_doc.__iter__.return_value = [mock_page]
    
    mock_fitz_open.return_value.__enter__.return_value = mock_doc
    
    test_pdf = tmp_path / "rotacion_test.pdf"
    test_pdf.touch()
    
    with patch("pathlib.Path.replace") as mock_replace:
        PDFToolbox.apply_physical_rotation(test_pdf, 90)
        
        mock_page.set_rotation.assert_called_once_with(90)
        mock_doc.save.assert_called_once()
        mock_replace.assert_called_once_with(test_pdf)


def test_apply_physical_rotation_zero_degrees(tmp_path: Path) -> None:
    """Verifica el aborto temprano si la rotación requerida es 0 (optimización estricta de I/O)."""
    test_pdf = tmp_path / "cero_grados.pdf"
    
    with patch("src.utils.pdf_tools.fitz.open") as mock_fitz_open:
        PDFToolbox.apply_physical_rotation(test_pdf, 0)
        mock_fitz_open.assert_not_called()


@patch("src.utils.pdf_tools.fitz.open")
def test_merge_by_folio_applies_rotation_and_overwrite(mock_fitz_open: MagicMock, tmp_path: Path) -> None:
    """Verifica que el ensamblaje respete la convención de nomenclatura exacta y elimine colisiones previas."""
    mock_result_doc = MagicMock()
    mock_sub_doc = MagicMock()
    mock_page = MagicMock()
    
    mock_page.rotation = 90
    mock_sub_doc.__iter__.return_value = [mock_page]
    
    mock_fitz_open.side_effect = [
        MagicMock(__enter__=MagicMock(return_value=mock_result_doc)),
        MagicMock(__enter__=MagicMock(return_value=mock_sub_doc))
    ]

    pages_data = [{"path": Path("dummy.pdf"), "rotation": 90}]
    output_dir = tmp_path
    
    target_file = output_dir / "FOLIO_123.pdf"
    target_file.touch()  # Simula un archivo preexistente para desencadenar deduplicación

    with patch("pathlib.Path.unlink") as mock_unlink:
        PDFToolbox.merge_by_folio(pages_data, "FOLIO_123.pdf", output_dir)
        
        # Validamos que el archivo original en disco se purgó para la sobrescritura limpia
        mock_unlink.assert_called_once()

    # 90 (actual) + 90 (instrucción) % 360 = 180
    mock_page.set_rotation.assert_called_with(180)
    mock_result_doc.insert_pdf.assert_called_with(mock_sub_doc)
    mock_result_doc.save.assert_called()