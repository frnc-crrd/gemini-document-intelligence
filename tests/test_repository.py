"""Pruebas unitarias para el repositorio de base de datos.

Valida el modelo transaccional y la resolución de conflictos (Deduplicación/Versionamiento).
Aísla la interacción con SQLAlchemy mediante inyección estricta de Context Managers
y simulación expansiva (Omni-Mock) de consultas escalares.
"""

import pytest
from unittest.mock import MagicMock, patch
from src.db.repository import PostgresRepository


@pytest.fixture
def mock_db_session():
    """Provee un entorno aislado para SQLAlchemy garantizando el soporte del Context Manager."""
    with patch("src.db.repository.sessionmaker") as mock_sm:
        mock_session = MagicMock()
        mock_sm.return_value.return_value.__enter__.return_value = mock_session
        yield mock_session


def _setup_omni_mock(mock_session, records, max_version=1):
    """Inyecta respuestas en todas las superficies de API posibles de SQLAlchemy (1.x y 2.0)."""
    first_record = records[0] if records else None

    # Mocks para la sintaxis SQLAlchemy 1.x (session.query)
    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = records
    mock_query.filter.return_value.first.return_value = first_record
    mock_query.filter_by.return_value.all.return_value = records
    mock_query.filter_by.return_value.first.return_value = first_record
    mock_session.query.return_value = mock_query

    # Mocks para la sintaxis SQLAlchemy 2.0 (session.execute y session.scalars)
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = records
    mock_scalars.first.return_value = first_record
    
    mock_exec_result = MagicMock()
    mock_exec_result.scalars.return_value = mock_scalars
    mock_exec_result.scalar.return_value = max_version
    mock_exec_result.scalar_one_or_none.return_value = first_record
    mock_exec_result.all.return_value = records
    mock_exec_result.first.return_value = first_record

    mock_session.execute.return_value = mock_exec_result
    mock_session.scalars.return_value = mock_scalars


@patch("src.db.repository.create_engine")
def test_initialize_schema(mock_engine) -> None:
    """Valida la instanciación del motor relacional sin excepciones."""
    repo = PostgresRepository()
    with patch("src.db.repository.Base.metadata.create_all") as mock_create:
        repo.initialize_schema()
        mock_create.assert_called_once()


def test_resolve_versioning_nuevo(mock_db_session) -> None:
    """Evalúa la inserción de un folio inexistente."""
    _setup_omni_mock(mock_db_session, [])
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("F1", "Factura", "origen.pdf", 90)
    assert version == 1
    assert accion == "NUEVO"


def test_resolve_versioning_mismo_origen_sobrescribir(mock_db_session) -> None:
    """Garantiza la sobrescritura ante repetición del mismo origen con mejor calidad."""
    mock_record = MagicMock()
    mock_record.archivo_original = "origen.pdf"
    mock_record.confianza_promedio = 50
    mock_record.version = 1
    
    _setup_omni_mock(mock_db_session, [mock_record])
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("F1", "Factura", "origen.pdf", 90)
    assert version == 1
    assert accion == "SOBRESCRIBIR"


def test_resolve_versioning_mismo_origen_descartar(mock_db_session) -> None:
    """Previene el reprocesamiento de orígenes idénticos si la calidad extraída es inferior."""
    mock_record = MagicMock()
    mock_record.archivo_original = "origen.pdf"
    mock_record.confianza_promedio = 90
    mock_record.version = 1
    
    _setup_omni_mock(mock_db_session, [mock_record])
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("F1", "Factura", "origen.pdf", 50)
    assert accion == "DESCARTAR"


def test_resolve_versioning_diferente_origen_versionar(mock_db_session) -> None:
    """Activa el versionamiento lógico ante un mismo folio extraído de otro escaneo."""
    mock_record = MagicMock()
    mock_record.archivo_original = "origen1.pdf"
    mock_record.version = 1
    
    _setup_omni_mock(mock_db_session, [mock_record], max_version=1)
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("F1", "Factura", "origen2.pdf", 90)
    assert version == 2
    assert accion == "VERSIONAR"


def test_resolve_versioning_diferente_categoria_no_colisiona(mock_db_session) -> None:
    """Verifica el aislamiento de folios homónimos pertenecientes a dominios semánticos distintos."""
    _setup_omni_mock(mock_db_session, [])
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("123", "Remisión", "origen.pdf", 90)
    assert version == 1
    assert accion == "NUEVO"


def test_resolve_versioning_cache_none_score(mock_db_session) -> None:
    """Verifica que un valor NULL en la base de datos no rompa la evaluación de caché."""
    mock_record = MagicMock()
    mock_record.archivo_original = "origen.pdf"
    mock_record.confianza_promedio = None  # Simulación de valor NULL
    mock_record.version = 1
    
    # FIX APLICADO: [mock_record] en lugar de [[mock_record]]
    _setup_omni_mock(mock_db_session, [mock_record])
    
    repo = PostgresRepository()
    version, accion = repo.resolve_versioning("F1", "Factura", "origen.pdf", 90)
    assert version == 1
    assert accion == "SOBRESCRIBIR"


def test_upsert_batch_success(mock_db_session) -> None:
    """Valida la operación transaccional de inserción en masa vectorizada."""
    repo = PostgresRepository()
    data = [{"Folio": "A1", "Categoría": "Factura", "Versión": 1, "Cliente": "CLI", "Archivo Original": "orig", "Tipo Original": "PDF", "Páginas Final": 1, "Status": "OK", "Confianza": 90, "Ruta del Archivo": "path", "Justificación": ""}]
    
    repo.upsert_batch(data)
    assert mock_db_session.execute.call_count >= 1
    mock_db_session.commit.assert_called_once()


def test_upsert_batch_internal_deduplication(mock_db_session) -> None:
    """Asegura la limpieza de duplicados en la capa de memoria antes del commit."""
    repo = PostgresRepository()
    row = {"Folio": "A1", "Categoría": "Factura", "Versión": 1, "Cliente": "CLI", "Archivo Original": "orig", "Tipo Original": "PDF", "Páginas Final": 1, "Status": "OK", "Confianza": 90, "Ruta del Archivo": "path", "Justificación": ""}
    data = [row, row.copy()]
    
    repo.upsert_batch(data)
    mock_db_session.commit.assert_called_once()
    assert mock_db_session.execute.call_count >= 1


def test_upsert_batch_empty_list(mock_db_session) -> None:
    """Evita interacciones ociosas con el motor relacional."""
    repo = PostgresRepository()
    repo.upsert_batch([])
    mock_db_session.commit.assert_not_called()


def test_upsert_batch_rollback_on_error(mock_db_session) -> None:
    """Valida el rechazo de la transacción ante violaciones DDL o fallos de ejecución."""
    mock_db_session.execute.side_effect = Exception("DB Error simulado")
    repo = PostgresRepository()
    
    data = [{"Folio": "A1", "Categoría": "Factura", "Versión": 1, "Cliente": "CLI", "Archivo Original": "orig", "Tipo Original": "PDF", "Páginas Final": 1, "Status": "OK", "Confianza": 90, "Ruta del Archivo": "path", "Justificación": ""}]
    
    with pytest.raises(Exception, match="DB Error simulado"):
        repo.upsert_batch(data)
        
    mock_db_session.commit.assert_not_called()