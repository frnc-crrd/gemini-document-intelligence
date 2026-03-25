"""Pruebas unitarias para el motor de inferencia (Analyzer).

Valida el comportamiento de la IA, el procesamiento OCR-Aware,
y la ejecución estricta del Cortacircuitos (Circuit Breaker) para
proteger la cuota de facturación de Google Cloud.
"""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from google.genai.errors import APIError

from src.core.analyzer import DocumentAnalyzer
from src.models import ExtractionResponse

@pytest.fixture
def analyzer() -> DocumentAnalyzer:
    """Inicializa el orquestador de IA con las dependencias externas aisladas."""
    with patch('src.core.analyzer.genai.Client'), \
         patch('src.core.analyzer.get_context_manager'):
        return DocumentAnalyzer()

def test_ejecutar_agente_success(analyzer: DocumentAnalyzer) -> None:
    """Verifica que el agente devuelva la estructura parseada correctamente en un escenario ideal."""
    mock_response = MagicMock()
    mock_response.parsed = ExtractionResponse(documents=[])
    analyzer.client.models.generate_content.return_value = mock_response

    result = analyzer._ejecutar_agente("test prompt", [("page_001.pdf", MagicMock())], ExtractionResponse)
    
    assert result is not None

def test_ejecutar_agente_retries_and_fails_on_exhaustion(analyzer: DocumentAnalyzer) -> None:
    """Verifica que el límite de reintentos aborte detonando el Cortacircuitos (Error 429)."""
    analyzer.client.models.generate_content.side_effect = APIError(
        "429 Resource has been exhausted (e.g. check quota).",
        {}
    )
    
    with pytest.raises(ConnectionAbortedError):
        analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)

def test_ejecutar_agente_aborts_immediately_on_fatal_error(analyzer: DocumentAnalyzer) -> None:
    """Verifica que errores de sintaxis o formato (ej. 400 Bad Request) aborten sin reintentar."""
    analyzer.client.models.generate_content.side_effect = APIError(
        "400 Bad Request",
        {}
    )
    
    result = analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)
    
    assert result is None

@patch('src.core.analyzer.PDFToolbox')
def test_analyze_batch_applies_physical_rotation(mock_pdf_tools: MagicMock, analyzer: DocumentAnalyzer) -> None:
    """Verifica que si la IA detecta que la imagen está rotada, se invoque la mutación física en disco."""
    # Uso de Mocks puros para evadir las restricciones estructurales de Pydantic
    mock_orientation = MagicMock()
    mock_orientation.parsed = MagicMock()
    ori_mock = MagicMock()
    ori_mock.file_name = "page_001.pdf"
    ori_mock.rotation_degrees = 90
    mock_orientation.parsed.orientations = [ori_mock]
    
    mock_extraction = MagicMock()
    mock_extraction.parsed = MagicMock()
    doc_mock = MagicMock()
    doc_mock.folios = ["A12052"]
    page_mock = MagicMock()
    page_mock.file_name = "page_001.pdf"
    page_mock.rotation_degrees = 0
    doc_mock.pages = [page_mock]
    doc_mock.document_type = "Factura"
    doc_mock.client_name = "Test"
    doc_mock.confidence_score = 99
    doc_mock.reasoning = "OK"
    mock_extraction.parsed.documents = [doc_mock]
    
    analyzer.client.models.generate_content.side_effect = [mock_orientation, mock_extraction]
    dummy_path = Path("fake_dir/test_page.pdf")
    
    with patch.object(analyzer, '_pdf_page_to_image', return_value=MagicMock()):
        analyzer.analyze_batch([dummy_path], "test_doc.pdf")
    
    mock_pdf_tools.apply_physical_rotation.assert_called_once_with(dummy_path, 90)