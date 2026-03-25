"""Pruebas unitarias para el generador de reportes."""

import pytest
from pathlib import Path
from unittest.mock import patch
from src.utils.report_generator import ReportGenerator

@pytest.fixture(autouse=True)
def reset_report_generator_state():
    """Resetea el estado en memoria para aislar las pruebas."""
    ReportGenerator._last_report_path = None
    yield
    ReportGenerator._last_report_path = None

def test_generate_excel_empty_data() -> None:
    assert ReportGenerator.generate_excel([]) is None

@patch("src.utils.report_generator.pd.DataFrame.to_excel")
def test_generate_excel_success(mock_to_excel, tmp_path: Path) -> None:
    """Valida la creación del reporte y el control del estado del archivo previo."""
    with patch("src.utils.report_generator.settings") as mock_settings:
        mock_settings.data_dir = tmp_path
        mock_settings.base_dir = tmp_path
        data = [{"Folio": "A1", "Categoría": "Factura", "Páginas Final": 1, "Status": "OK"}]
        
        result_path = ReportGenerator.generate_excel(data)
        
        assert result_path is not None
        mock_to_excel.assert_called_once()
        assert ReportGenerator._last_report_path == result_path

@patch("src.utils.report_generator.pd.DataFrame.to_excel")
def test_generate_excel_permission_error(mock_to_excel) -> None:
    mock_to_excel.side_effect = PermissionError("Simulated File Open")
    data = [{"Folio": "A1"}]
    assert ReportGenerator.generate_excel(data) is None