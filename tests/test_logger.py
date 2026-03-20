"""Pruebas unitarias para el módulo de registro estructurado (Logging) Dual.

Valida la correcta serialización de eventos a JSON, la aplicación de formatos
de color en consola y el manejo de instancias únicas de los manejadores.
"""

import io
import json
import logging
import pytest
from typing import Dict, Any, Tuple

from src.core.logger import get_system_logger, JSONFormatter, ColoredConsoleFormatter


@pytest.fixture
def dual_logger() -> Tuple[logging.Logger, io.StringIO, io.StringIO]:
    """Provee una instancia limpia del logger conectada a streams en memoria.

    Returns:
        Tuple[logging.Logger, io.StringIO, io.StringIO]: Instancia del logger,
        buffer de consola y buffer de archivo.
    """
    logger_name = "test_dual_logger"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.handlers.clear()
    
    console_stream = io.StringIO()
    console_handler = logging.StreamHandler(console_stream)
    console_handler.setFormatter(ColoredConsoleFormatter())
    logger.addHandler(console_handler)
    
    file_stream = io.StringIO()
    file_handler = logging.StreamHandler(file_stream)
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)
    
    return logger, console_stream, file_stream


def test_json_formatter_structure(dual_logger: Tuple[logging.Logger, io.StringIO, io.StringIO]) -> None:
    """Verifica que el stream de auditoría se serialice en JSON estricto."""
    logger, console_stream, file_stream = dual_logger
    test_message = "Auditoria de subsistema"
    logger.info(test_message)

    log_output = file_stream.getvalue().strip()
    assert log_output != ""
    
    try:
        log_data: Dict[str, Any] = json.loads(log_output)
    except json.JSONDecodeError as e:
        pytest.fail(f"El log emitido no es un JSON válido: {e}")

    assert log_data["level"] == "INFO"
    assert log_data["message"] == test_message
    assert log_data["logger"] == "test_dual_logger"
    assert "timestamp" in log_data


def test_colored_console_formatter(dual_logger: Tuple[logging.Logger, io.StringIO, io.StringIO]) -> None:
    """Verifica la inyección de colores ANSI y la estructura plana en la consola."""
    logger, console_stream, file_stream = dual_logger
    test_message = "Fallo de conexión"
    logger.error(test_message)
    
    console_output = console_stream.getvalue().strip()
    
    assert "\033[31m" in console_output
    assert "ERROR" in console_output
    assert test_message in console_output
    assert "{" not in console_output


def test_logger_singleton_handlers() -> None:
    """Garantiza que llamadas repetidas no multipliquen los manejadores."""
    logger_name = "duplicate_handler_test"
    logger1 = get_system_logger(logger_name)
    logger2 = get_system_logger(logger_name)

    assert logger1 is logger2
    assert len(logger1.handlers) == 2