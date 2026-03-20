"""Pruebas unitarias para el gestor de contexto y almacenamiento.

Valida el comportamiento de las estrategias de I/O de manera aislada usando mocks
y verifica que la actualización de memoria no corrompa los esquemas existentes.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from src.core.context import (
    LocalStorageStrategy, 
    S3StorageStrategy, 
    SystemContextManager
)
from src.models import LogicalDocument, PageInstruction


@pytest.fixture
def mock_document() -> LogicalDocument:
    """Retorna un documento procesado válido para pruebas de actualización."""
    return LogicalDocument(
        folios=["F-12345"],
        pages=[PageInstruction(file_name="test.pdf", rotation_degrees=0)],
        document_type="Factura",
        client_name="Microsip Corp",
        confidence_score=95,
        reasoning="Análisis validado"
    )


def test_local_storage_strategy_load_missing(tmp_path: Path) -> None:
    """Valida que la ausencia de archivo local devuelva None silenciosamente."""
    file_path = tmp_path / "missing.json"
    strategy = LocalStorageStrategy(file_path)
    result = strategy.load()
    assert result is None


def test_local_storage_strategy_corrupted_json(tmp_path: Path) -> None:
    """Valida que un JSON inválido sea atrapado e ignorado para prevenir caídas."""
    file_path = tmp_path / "corrupt.json"
    file_path.write_text("{esta_no_es_una_estructura_valida:")
    
    strategy = LocalStorageStrategy(file_path)
    result = strategy.load()
    assert result is None


def test_local_storage_strategy_save_and_load(tmp_path: Path) -> None:
    """Verifica el flujo correcto de escritura y lectura local."""
    file_path = tmp_path / "context.json"
    strategy = LocalStorageStrategy(file_path)
    
    test_data = {"version": "test", "val": 1}
    strategy.save(test_data)
    
    loaded_data = strategy.load()
    assert loaded_data == test_data


def test_s3_storage_strategy_client_error() -> None:
    """Verifica que fallos de red o credenciales en S3 se intercepten."""
    mock_s3 = MagicMock()
    # Inyección de clase derivada de Exception para evitar TypeError en el bloque except
    mock_s3.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
    
    error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
    
    strategy = S3StorageStrategy(mock_s3, "test-bucket", "key.json")
    result = strategy.load()
    
    assert result is None
    mock_s3.get_object.assert_called_once()


def test_system_context_manager_update(mock_document: LogicalDocument) -> None:
    """Asegura que actualizar el contexto expanda catálogos y métricas sin perder datos."""
    mock_strategy = MagicMock()
    mock_strategy.load.return_value = None
    
    manager = SystemContextManager(mock_strategy)
    
    ctx_inicial = manager.obtener_contexto_actual()
    assert ctx_inicial["estadisticas"]["total_documentos_procesados"] == 0
    
    manager.actualizar_contexto(mock_document)
    
    assert mock_strategy.save.call_count == 1
    saved_context = mock_strategy.save.call_args[0][0]
    
    assert saved_context["estadisticas"]["total_documentos_procesados"] == 1
    assert "F" in saved_context["patrones_folio"]
    assert "MICROSIP CORP" in saved_context["clientes_conocidos"]
    assert saved_context["clientes_conocidos"]["MICROSIP CORP"]["frecuencia"] == 1