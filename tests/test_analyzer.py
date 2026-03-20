"""Pruebas unitarias para el motor de análisis y comunicación de LLM.

Utiliza simulaciones estandarizadas para aislar la lógica de red externa,
garantizando que no se realicen peticiones reales durante la validación.
"""

import pytest
from typing import Generator
from unittest.mock import MagicMock, patch

from google.genai.errors import APIError

from src.core.analyzer import DocumentAnalyzer
from src.models import ExtractionResponse, LogicalDocExtraction


@pytest.fixture
def analyzer() -> Generator[DocumentAnalyzer, None, None]:
    """Provee una instancia del analizador con dependencias aisladas.
    
    Emplea yield para mantener activos los parches en memoria durante
    el ciclo de vida completo de cada test independiente.
    """
    with patch('src.core.analyzer.get_context_manager'):
        with patch('src.core.analyzer.settings') as mock_settings:
            # Forzamos parámetros deterministas para evitar latencia en pruebas
            mock_settings.gemini_api_key = "fake_key"
            mock_settings.max_retries = 2
            mock_settings.api_delay = 0.01
            
            with patch('src.core.analyzer.genai.Client'):
                yield DocumentAnalyzer()


def test_ejecutar_agente_success(analyzer: DocumentAnalyzer) -> None:
    """Verifica el retorno correcto cuando la API responde de forma estructurada."""
    mock_response = MagicMock()
    mock_response.parsed = ExtractionResponse(documents=[
        LogicalDocExtraction(
            folios=["123"], ordered_file_names=["a.pdf"], 
            document_type="Test", confidence_score=99, reasoning="Test"
        )
    ])
    analyzer.client.models.generate_content.return_value = mock_response

    result = analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)
    
    assert result is not None
    assert result.documents[0].folios == ["123"]


def test_ejecutar_agente_retries_and_fails(analyzer: DocumentAnalyzer) -> None:
    """Verifica que el límite de reintentos aborte devolviendo None en fallos de red persistentes."""
    analyzer.client.models.generate_content.side_effect = APIError(
        "Quota exceeded", 429, None
    )

    result = analyzer._ejecutar_agente("test prompt", [("name", MagicMock())], ExtractionResponse)
    
    assert result is None
    assert analyzer.client.models.generate_content.call_count == 2