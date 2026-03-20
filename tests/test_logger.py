"""Pruebas unitarias para el módulo de registro estructurado (Logging).

Valida la correcta serialización de eventos a JSON, el manejo de niveles
de severidad y la captura precisa de trazas de excepciones.
"""

import io
import json
import logging
import pytest
from typing import Dict, Any, Tuple

from src.core.logger import get_system_logger, JSONFormatter


@pytest.fixture
def custom_logger() -> Tuple[logging.Logger, io.StringIO]:
    """Provee una instancia limpia del logger conectada a un stream en memoria.
    
    Returns:
        Tuple[logging.Logger, io.StringIO]: Instancia del logger y el buffer de salida.
    """
    logger_name = "test_logger_unit_stream"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers.clear()
    
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    
    return logger, stream


def test_json_formatter_structure(custom_logger: Tuple[logging.Logger, io.StringIO]) -> None:
    """Verifica que un mensaje informativo se serialice en un JSON válido con atributos requeridos."""
    logger, stream = custom_logger
    test_message = "Prueba de inicialización de subsistema"
    logger.info(test_message)

    log_output = stream.getvalue().strip()

    assert log_output != ""
    
    try:
        log_data: Dict[str, Any] = json.loads(log_output)
    except json.JSONDecodeError as e:
        pytest.fail(f"El log emitido no es un JSON válido: {e}")

    assert log_data["level"] == "INFO"
    assert log_data["message"] == test_message
    assert log_data["logger"] == "test_logger_unit_stream"
    assert "timestamp" in log_data
    assert "module" in log_data
    assert "function" in log_data
    assert "line" in log_data


def test_logger_exception_capture(custom_logger: Tuple[logging.Logger, io.StringIO]) -> None:
    """Verifica que las trazas de excepciones (stack trace) se incluyan correctamente en el payload."""
    logger, stream = custom_logger
    try:
        raise ValueError("Error de validación de esquema simulado")
    except ValueError:
        logger.error("Fallo crítico detectado", exc_info=True)

    log_output = stream.getvalue().strip()
    log_data = json.loads(log_output)

    assert log_data["level"] == "ERROR"
    assert log_data["message"] == "Fallo crítico detectado"
    assert "exception" in log_data
    assert "ValueError: Error de validación de esquema simulado" in log_data["exception"]


def test_logger_singleton_handlers() -> None:
    """Garantiza que múltiples invocaciones a get_system_logger no agreguen manejadores redundantes."""
    logger_name = "duplicate_handler_test"
    logger1 = get_system_logger(logger_name)
    logger2 = get_system_logger(logger_name)

    assert logger1 is logger2
    assert len(logger1.handlers) == 1