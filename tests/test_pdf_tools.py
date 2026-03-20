"""Pruebas unitarias para utilidades físicas de manipulación de PDF.

Se utilizan mocks sobre la librería PyMuPDF (fitz) para evitar la creación
de archivos físicos innecesarios en el disco durante la ejecución de los tests.
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
    
    # Configurar el manejador de contexto
    mock_fitz_open.return_value.__enter__.return_value = mock_doc

    input_pdf = tmp_path / "test_doc.pdf"
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    result = PDFToolbox.explode_pdf(input_pdf, output_dir)

    assert len(result) == 2
    assert "test_doc_P001_T002.pdf" in result[0].name
    assert "test_doc_P002_T002.pdf" in result[1].name


@patch("src.utils.pdf_tools.fitz.open")
def test_merge_by_folio_applies_rotation(mock_fitz_open: MagicMock, tmp_path: Path) -> None:
    """Verifica que la metadata de rotación se sume correctamente en el documento de salida."""
    mock_result_doc = MagicMock()
    mock_sub_doc = MagicMock()
    mock_page = MagicMock()
    
    mock_page.rotation = 90  # Rotación base
    mock_sub_doc.__iter__.return_value = [mock_page]
    
    # Manejador de contexto alternando entre el documento destino y el documento origen
    mock_fitz_open.side_effect = [
        MagicMock(__enter__=MagicMock(return_value=mock_result_doc)),  # El documento final
        MagicMock(__enter__=MagicMock(return_value=mock_sub_doc))      # El documento sub-página
    ]

    pages_data = [{"path": Path("dummy.pdf"), "rotation": 90}]
    output_dir = tmp_path

    PDFToolbox.merge_by_folio(pages_data, "FOLIO_123", output_dir)

    # 90 (actual) + 90 (instrucción) % 360 = 180
    mock_page.set_rotation.assert_called_with(180)
    mock_result_doc.insert_pdf.assert_called_with(mock_sub_doc)
    mock_result_doc.save.assert_called()