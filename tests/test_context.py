"""Pruebas unitarias para el gestor de memoria/contexto."""

import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock
from src.core.context import LocalStorageStrategy, S3StorageStrategy, SystemContextManager

@pytest.fixture
def mock_metadata_dict() -> dict:
    return {
        "Folio": "A12052",
        "Categoría": "Factura",
        "Cliente": "VILLAS",
        "Confianza": 95,
        "Archivo Original": "test.pdf"
    }

def test_local_storage_strategy_load_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "missing.json"
    strategy = LocalStorageStrategy(file_path)
    assert strategy.load() is None

def test_local_storage_strategy_corrupted_json(tmp_path: Path) -> None:
    file_path = tmp_path / "corrupt.json"
    file_path.write_text("{bad json")
    strategy = LocalStorageStrategy(file_path)
    assert strategy.load() is None

def test_local_storage_strategy_save_and_load(tmp_path: Path) -> None:
    file_path = tmp_path / "context.json"
    strategy = LocalStorageStrategy(file_path)
    data = {"test": "data"}
    strategy.save(data)
    loaded = strategy.load()
    assert loaded == data

def test_s3_storage_strategy_client_error() -> None:
    mock_client = MagicMock()
    from botocore.exceptions import ClientError
    mock_client.get_object.side_effect = ClientError({"Error": {"Message": "Not Found"}}, "GetObject")
    
    class NoSuchKeyMock(Exception):
        pass
        
    mock_client.exceptions.NoSuchKey = NoSuchKeyMock
    strategy = S3StorageStrategy(mock_client, "bucket", "key")
    assert strategy.load() is None

def test_system_context_manager_update(mock_metadata_dict: dict) -> None:
    """Asegura que actualizar el contexto expanda catálogos (Cliente, Folio, Tipo)."""
    mock_strategy = MagicMock()
    mock_strategy.load.return_value = None
    
    manager = SystemContextManager(mock_strategy)
    ctx_inicial = manager.obtener_contexto_actual()
    assert ctx_inicial["estadisticas"]["total_documentos_procesados"] == 0
    
    manager.actualizar_contexto(mock_metadata_dict)
    
    assert mock_strategy.save.call_count == 1
    saved_context = mock_strategy.save.call_args[0][0]
    
    assert saved_context["estadisticas"]["total_documentos_procesados"] == 1
    assert "A" in saved_context["patrones_folio"]
    assert saved_context["patrones_folio"]["A"]["tipo_asociado"] == "Factura"
    assert "VILLAS" in saved_context["clientes_conocidos"]