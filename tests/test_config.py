"""Pruebas unitarias para la configuración del sistema.

Aplica limpieza de caché (Singleton) para asegurar que el entorno
sea evaluado limpiamente en cada prueba aislada.
"""

import pytest
from pydantic import ValidationError
from src.config import get_settings, Settings


@pytest.fixture(autouse=True)
def clear_settings_cache():
    """Limpia el caché del Singleton de configuración antes y después de cada prueba."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_settings_successful_initialization(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica la carga exitosa cuando los parámetros requeridos existen."""
    monkeypatch.setenv("GEMINI_API_KEY", "test_api_key_123")
    monkeypatch.setenv("EXECUTION_MODE", "cloud")
    
    settings = get_settings()
    
    assert settings.gemini_api_key == "test_api_key_123"
    assert settings.execution_mode == "cloud"
    assert settings.dpi_conversion == 200
    assert settings.max_retries == 3
    assert settings.max_threads == 3
    assert settings.pdf_chunk_max_pages == 100
    assert settings.physical_chunk_size == 50


def test_settings_missing_api_key_raises_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Asegura que el sistema aborte si no hay API Key de Gemini."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValidationError):
        # Desactiva la carga del archivo local .env en memoria para forzar el fallo de validación
        Settings(_env_file=None) # type: ignore


def test_settings_dynamic_paths_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que las rutas relativas se construyan correctamente."""
    monkeypatch.setenv("GEMINI_API_KEY", "test")
    settings = get_settings()
    
    assert settings.data_dir.name == "data"
    assert settings.raw_dir.name == "01_raw"
    assert settings.processed_dir.name == "04_processed"


def test_settings_validation_constraints(monkeypatch: pytest.MonkeyPatch) -> None:
    """Garantiza que no se puedan inyectar resoluciones (DPI) inválidas."""
    monkeypatch.setenv("GEMINI_API_KEY", "test")
    monkeypatch.setenv("DPI_CONVERSION", "50")
    with pytest.raises(ValidationError):
        get_settings()


def test_settings_concurrency_constraints(monkeypatch: pytest.MonkeyPatch) -> None:
    """Garantiza que la concurrencia sea válida para proteger la red."""
    monkeypatch.setenv("GEMINI_API_KEY", "test")
    monkeypatch.setenv("MAX_THREADS", "0")
    with pytest.raises(ValidationError):
        get_settings()