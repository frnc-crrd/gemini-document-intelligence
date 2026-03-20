"""Pruebas unitarias para el módulo de configuración del sistema.

Valida la correcta carga de variables de entorno, la inicialización
de rutas dinámicas y el comportamiento ante la ausencia de credenciales.
"""

import pytest
from pydantic import ValidationError
from pathlib import Path
from src.config import Settings, get_settings


def test_settings_successful_initialization(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica la carga exitosa cuando los parámetros requeridos existen."""
    monkeypatch.setenv("GEMINI_API_KEY", "test_api_key_123")
    monkeypatch.setenv("EXECUTION_MODE", "cloud")
    
    settings = get_settings()
    
    assert settings.gemini_api_key == "test_api_key_123"
    assert settings.execution_mode == "cloud"
    assert settings.dpi_conversion == 200
    assert settings.max_retries == 3


def test_settings_missing_api_key_raises_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verifica que el sistema falle tempranamente si falta la clave de API."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    # Aisla la prueba cambiando el directorio de trabajo para prevenir la lectura de .env locales
    monkeypatch.chdir(tmp_path)
    
    with pytest.raises(ValidationError) as exc_info:
        get_settings()
        
    assert "gemini_api_key" in str(exc_info.value)


def test_settings_dynamic_paths_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que las propiedades de ruta resuelvan la estructura esperada."""
    monkeypatch.setenv("GEMINI_API_KEY", "test_api_key_123")
    settings = get_settings()
    
    base_path = settings.base_dir
    
    assert isinstance(base_path, Path)
    assert settings.data_dir == base_path / "data"
    assert settings.raw_dir == settings.data_dir / "01_raw"
    assert settings.explosion_dir == settings.data_dir / "02_explosion"
    assert settings.final_dir == settings.data_dir / "03_final"


def test_settings_validation_constraints(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica las restricciones de tipo y límites lógicos."""
    monkeypatch.setenv("GEMINI_API_KEY", "test_api_key_123")
    monkeypatch.setenv("DPI_CONVERSION", "50")
    
    with pytest.raises(ValidationError) as exc_info:
        get_settings()
        
    assert "dpi_conversion" in str(exc_info.value)