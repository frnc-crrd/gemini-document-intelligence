"""Pruebas unitarias para el generador de reportes tabulares.

Valida la correcta instanciación de DataFrames, la inyección de dependencias
faltantes y el manejo de excepciones del sistema operativo (bloqueos).
"""

import pytest
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock

from src.utils.report_generator import ReportGenerator


@pytest.fixture
def sample_processed_data() -> List[Dict[str, Any]]:
    """Provee un lote de datos de prueba."""
    return [
        {
            "Folio": "TEST_123",
            "Categoría": "Factura",
            "Cliente": "Microsip",
            "Archivo Original": "doc.pdf",
            "Tipo Original": "PDF",
            "Páginas Original": 1,
            "Páginas Final": 1,
            "Status": "OK",
            "Confianza": 99,
            "Ruta del Archivo": "Factura/test.pdf",
            "Justificación": "Validado."
        }
    ]


def test_generate_excel_empty_data() -> None:
    """Verifica el aborto temprano cuando se provee un lote vacío."""
    result = ReportGenerator.generate_excel([])
    assert result is None


@patch("src.utils.report_generator.pd.DataFrame.to_excel")
@patch("src.utils.report_generator.settings")
def test_generate_excel_success(mock_settings: MagicMock, mock_to_excel: MagicMock, sample_processed_data: List[Dict[str, Any]], tmp_path: Path) -> None:
    """Verifica la orquestación correcta de Pandas para la exportación."""
    mock_settings.data_dir = tmp_path
    mock_settings.base_dir = tmp_path.parent
    
    result = ReportGenerator.generate_excel(sample_processed_data)
    
    assert result is not None
    assert result.parent == tmp_path
    assert result.suffix == ".xlsx"
    mock_to_excel.assert_called_once()


@patch("src.utils.report_generator.pd.DataFrame.to_excel")
@patch("src.utils.report_generator.settings")
def test_generate_excel_permission_error(mock_settings: MagicMock, mock_to_excel: MagicMock, sample_processed_data: List[Dict[str, Any]], tmp_path: Path) -> None:
    """Asegura la captura correcta de fallos de I/O bloqueante (File Lock)."""
    mock_settings.data_dir = tmp_path
    mock_settings.base_dir = tmp_path.parent
    
    mock_to_excel.side_effect = PermissionError("Permission denied")
    
    result = ReportGenerator.generate_excel(sample_processed_data)
    
    assert result is None