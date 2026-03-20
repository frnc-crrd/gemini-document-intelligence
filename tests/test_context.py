"""Pruebas unitarias para el gestor de contexto y almacenamiento.

Valida la interacción con diccionarios tipados genéricamente, eliminando
la dependencia estricta a modelos Pydantic obsoletos.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

from src.core.context import (
    LocalStorageStrategy, 
    S3StorageStrategy, 
    SystemContextManager
)


@pytest.fixture
def mock_metadata_dict() -> dict:
    """Retorna un diccionario procesado válido para pruebas de actualización."""
    return {
        "Folio": "F-12345",
        "Divisa": "MXN",
        "Cliente": "Microsip Corp",
        "Archivo Original": "lote.pdf",
        "Páginas Consolidado": 2,
        "Status": "OK",
        "Confianza Promedio": 95,
        "Ruta Servidor": "/ruta/final.pdf",
        "Justificación": "Validado."
    }


def test_local_storage_strategy_load_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "missing.json"
    strategy = LocalStorageStrategy(file_path)
    result = strategy.load()
    assert result is None


def test_local_storage_strategy_corrupted_json(tmp_path: Path) -> None:
    file_path = tmp_path / "corrupt.json"
    file_path.write_text("{esta_no_es_una_estructura_valida:")
    
    strategy = LocalStorageStrategy(file_path)
    result = strategy.load()
    assert result is None


def test_local_storage_strategy_save_and_load(tmp_path: Path) -> None:
    file_path = tmp_path / "context.json"
    strategy = LocalStorageStrategy(file_path)
    
    test_data = {"version": "test", "val": 1}
    strategy.save(test_data)
    
    loaded_data = strategy.load()
    assert loaded_data == test_data


def test_s3_storage_strategy_client_error() -> None:
    mock_s3 = MagicMock()
    
    # Inyectamos una clase base de Exception válida en el Mock para evitar
    # que la Máquina Virtual de Python arroje TypeError al evaluar el bloque except.
    class MockNoSuchKeyException(Exception):
        pass
        
    mock_s3.exceptions.NoSuchKey = MockNoSuchKeyException
    
    error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
    
    strategy = S3StorageStrategy(mock_s3, "test-bucket", "key.json")
    result = strategy.load()
    
    assert result is None
    mock_s3.get_object.assert_called_once()


def test_system_context_manager_update(mock_metadata_dict: dict) -> None:
    """Asegura que actualizar el contexto expanda catálogos usando diccionarios."""
    mock_strategy = MagicMock()
    mock_strategy.load.return_value = None
    
    manager = SystemContextManager(mock_strategy)
    
    ctx_inicial = manager.obtener_contexto_actual()
    assert ctx_inicial["estadisticas"]["total_documentos_procesados"] == 0
    
    manager.actualizar_contexto(mock_metadata_dict)
    
    assert mock_strategy.save.call_count == 1
    saved_context = mock_strategy.save.call_args[0][0]
    
    assert saved_context["estadisticas"]["total_documentos_procesados"] == 1
    assert "F" in saved_context["patrones_folio"]
    assert "MICROSIP CORP" in saved_context["clientes_conocidos"]
    assert saved_context["clientes_conocidos"]["MICROSIP CORP"]["divisa_comun"] == "MXN"