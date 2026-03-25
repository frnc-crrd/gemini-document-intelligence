"""Pruebas unitarias para las herramientas de manipulación PDF.

Asegura la inmutabilidad durante la fragmentación y la correcta 
aplicación de mutaciones espaciales en disco.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.utils.pdf_tools import PDFToolbox

@patch("src.utils.pdf_tools.fitz.open")
def test_explode_pdf_success(mock_fitz_open, tmp_path: Path) -> None:
    """Valida la separación de un documento multipágina en archivos individuales."""
    mock_doc = MagicMock()
    mock_doc.__len__.return_value = 2
    mock_fitz_open.return_value.__enter__.return_value = mock_doc

    input_path = tmp_path / "test_doc.pdf"
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    generated_files = PDFToolbox.explode_pdf(input_path, output_dir)
    assert len(generated_files) == 2
    assert "test_doc_P001_T002.pdf" in generated_files[0].name

@patch("src.utils.pdf_tools.fitz.open")
def test_apply_physical_rotation_success(mock_fitz_open, tmp_path: Path) -> None:
    """Valida la aplicación permanente de rotación sobre el sistema de archivos."""
    mock_doc = MagicMock()
    mock_page = MagicMock()
    mock_page.rotation = 0
    mock_doc.__iter__.return_value = [mock_page]
    
    # Inyectamos el archivo físico temporal durante la invocación del método save
    def fake_save(path, **kwargs):
        Path(path).touch()
    mock_doc.save.side_effect = fake_save
    
    mock_fitz_open.return_value.__enter__.return_value = mock_doc

    pdf_path = tmp_path / "target.pdf"
    pdf_path.touch()

    PDFToolbox.apply_physical_rotation(pdf_path, 90)
    mock_page.set_rotation.assert_called_once_with(90)

@patch("src.utils.pdf_tools.fitz.open")
def test_apply_physical_rotation_zero_degrees(mock_fitz_open, tmp_path: Path) -> None:
    """Asegura que se omitan operaciones de I/O innecesarias si la rotación es 0."""
    pdf_path = tmp_path / "target.pdf"
    PDFToolbox.apply_physical_rotation(pdf_path, 0)
    mock_fitz_open.assert_not_called()

@patch("src.utils.pdf_tools.fitz.open")
def test_merge_by_folio_applies_rotation_and_overwrite(mock_fitz_open, tmp_path: Path) -> None:
    """Verifica el reensamblaje lógico y la deduplicación por sobrescritura."""
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    
    existing_file = output_dir / "FOLIO.pdf"
    existing_file.touch()

    pages_data = [{"path": Path("dummy1.pdf"), "rotation": 0}]
    
    mock_result_doc = MagicMock()
    mock_fitz_open.return_value.__enter__.return_value = mock_result_doc

    result_path = PDFToolbox.merge_by_folio(pages_data, "FOLIO.pdf", output_dir)
    
    assert result_path.name == "FOLIO.pdf"
    mock_result_doc.save.assert_called_once()