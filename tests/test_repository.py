"""Pruebas unitarias para la capa de persistencia (Patrón Repositorio).

Valida el motor de reglas de deduplicación y versionamiento, el manejo 
de transacciones UPSERT y la purga pre-transaccional de colisiones considerando
la nueva heurística de aislamiento por categoría documental y la vectorización.
"""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

from src.db.repository import PostgresRepository


@pytest.fixture
def repo() -> PostgresRepository:
    with patch('src.db.repository.create_engine'), patch('src.db.repository.sessionmaker'):
        return PostgresRepository()


def test_initialize_schema(repo: PostgresRepository) -> None:
    with patch('src.db.repository.Base.metadata.create_all') as mock_create_all:
        repo.initialize_schema()
        mock_create_all.assert_called_once_with(bind=repo.engine)


def test_resolve_versioning_nuevo(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    mock_session.query().filter_by().all.return_value = []
    
    version, action = repo.resolve_versioning("F-123", "Factura", "origen.pdf", 90)
    assert version == 1
    assert action == "NUEVO"


def test_resolve_versioning_mismo_origen_sobrescribir(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    
    mock_registro = MagicMock()
    mock_registro.archivo_original = "origen.pdf"
    mock_registro.version = 1
    mock_registro.confianza_promedio = 80
    mock_session.query().filter_by().all.return_value = [mock_registro]
    
    version, action = repo.resolve_versioning("F-123", "Remisión", "origen.pdf", 90)
    assert version == 1
    assert action == "SOBRESCRIBIR"


def test_resolve_versioning_mismo_origen_descartar(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    
    mock_registro = MagicMock()
    mock_registro.archivo_original = "origen.pdf"
    mock_registro.version = 1
    mock_registro.confianza_promedio = 95
    mock_session.query().filter_by().all.return_value = [mock_registro]
    
    version, action = repo.resolve_versioning("F-123", "Factura", "origen.pdf", 80)
    assert version == 1
    assert action == "DESCARTAR"


def test_resolve_versioning_diferente_origen_versionar(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    
    mock_registro = MagicMock()
    mock_registro.archivo_original = "otro_origen.pdf"
    mock_registro.version = 2
    mock_session.query().filter_by().all.return_value = [mock_registro]
    
    version, action = repo.resolve_versioning("F-123", "Remisión", "nuevo_origen.pdf", 90)
    assert version == 3
    assert action == "NUEVO"


def test_resolve_versioning_diferente_categoria_no_colisiona(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    mock_session.query().filter_by().all.return_value = []
    
    version, action = repo.resolve_versioning("F-123", "Factura", "nuevo_origen.pdf", 90)
    
    assert version == 1
    assert action == "NUEVO"


def test_upsert_batch_success(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session

    test_data = [{
        "Folio": "F-TEST-1", "Categoría": "Factura", "Versión": 1, "Divisa": "MXN",
        "Archivo Original": "test.pdf", "Status": "OK",
        "Ruta del Archivo": "/ruta/test.pdf"
    }]

    repo.upsert_batch(test_data)
    
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


def test_upsert_batch_internal_deduplication(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session

    test_data = [
        {"Folio": "DUP-1", "Categoría": "Factura", "Versión": 1, "Archivo Original": "origen.pdf", "Confianza": 80},
        {"Folio": "DUP-1", "Categoría": "Factura", "Versión": 1, "Archivo Original": "origen.pdf", "Confianza": 95},
    ]

    repo.upsert_batch(test_data)
    
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


def test_upsert_batch_empty_list(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session

    repo.upsert_batch([])

    mock_session.execute.assert_not_called()
    mock_session.commit.assert_not_called()


def test_upsert_batch_rollback_on_error(repo: PostgresRepository) -> None:
    mock_session = MagicMock()
    repo.SessionLocal.return_value.__enter__.return_value = mock_session
    mock_session.execute.side_effect = SQLAlchemyError("Simulated DB Lock Error")

    test_data = [{"Folio": "F-TEST-2", "Categoría": "Factura", "Ruta del Archivo": "/ruta/test2.pdf"}]

    repo.upsert_batch(test_data)

    mock_session.rollback.assert_called_once()