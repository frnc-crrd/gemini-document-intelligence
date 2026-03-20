"""Pruebas unitarias para el motor de análisis y comunicación de LLM.

Utiliza simulaciones estandarizadas para aislar la lógica de red externa.
"""

import pytest
from typing import Generator
from unittest.mock import MagicMock, patch
from src.core.analyzer import DocumentAnalyzer
from src.models import ExtractionResponse, LogicalDocExtraction, OrientationResponse, PageOrientation, PageRole
from google.genai.errors import APIError


@pytest.fixture
def analyzer() -> Generator[DocumentAnalyzer, None, None]:
    """Provee una instancia del analizador con dependencias aisladas."""
    with patch('src.core.analyzer.get_context_manager'), \
         patch('src.core.analyzer.settings') as mock_settings, \
         patch('src.core.analyzer.genai.Client'):
         
        mock_settings.gemini_api_key = "fake_key_123"
        mock_settings.max_retries = 2
        mock_settings.api_delay = 0.01
        mock_settings.gemini_model = "test-model"
        
        yield DocumentAnalyzer()


def test_ejecutar_agente_success(analyzer: DocumentAnalyzer) -> None:
    """Verifica el retorno correcto cuando la API responde de forma estructurada con la heurística de límites."""
    mock_response = MagicMock()
    mock_response.parsed = ExtractionResponse(documents=[
        LogicalDocExtraction(
            folios=["123"], 
            page_roles=[PageRole(file_name="a.pdf", role="UNICA", evidence="Contiene encabezado y sellos.")],
            ordered_file_names=["a.pdf"], 
            document_type="Test", 
            confidence_score=99, 
            reasoning="Test"
        )
    ])
    analyzer.client.models.generate_content.return_value = mock_response

    result = analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)
    
    assert result is not None
    assert result.documents[0].folios == ["123"]
    assert result.documents[0].page_roles[0].role == "UNICA"


def test_ejecutar_agente_retries_and_fails(analyzer: DocumentAnalyzer) -> None:
    """Verifica que el límite de reintentos aborte devolviendo None en fallos persistentes."""
    analyzer.client.models.generate_content.side_effect = APIError(
        "429 Resource has been exhausted (e.g. check quota).",
        {}
    )

    result = analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)
    
    assert result is None
    assert analyzer.client.models.generate_content.call_count == 2


@patch("src.utils.pdf_tools.PDFToolbox.apply_physical_rotation")
@patch("src.core.analyzer.DocumentAnalyzer._pdf_page_to_image")
def test_analyze_batch_applies_physical_rotation(mock_rasterize: MagicMock, mock_rotate: MagicMock, analyzer: DocumentAnalyzer) -> None:
    """Valida la inyección de la mutación física basada en la heurística OCR-Aware."""
    mock_img = MagicMock()
    mock_img.rotate.return_value = mock_img
    mock_rasterize.return_value = mock_img
    
    mock_orientacion_res = MagicMock()
    mock_orientacion_res.parsed = OrientationResponse(orientations=[
        PageOrientation(file_name="page_001.pdf", rotation_degrees=90, reasoning="Test")
    ])
    
    mock_extraction_res = MagicMock()
    mock_extraction_res.parsed = ExtractionResponse(documents=[
        LogicalDocExtraction(
            folios=["123"],
            page_roles=[PageRole(file_name="page_001.pdf", role="UNICA", evidence="Encabezado y sellos presentes.")],
            ordered_file_names=["page_001.pdf"], 
            document_type="Test", 
            confidence_score=99, 
            reasoning="Test"
        )
    ])
    
    analyzer.client.models.generate_content.side_effect = [mock_orientacion_res, mock_extraction_res]
    
    mock_path = MagicMock()
    mock_path.name = "doc.pdf"
    mock_path.suffix = ".pdf"
    
    response = analyzer.analyze_batch([mock_path], "origen.pdf")
    
    mock_rotate.assert_called_once_with(mock_path, 90)
    assert response.documents[0].pages[0].rotation_degrees == 0