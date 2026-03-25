"""Pruebas unitarias para el módulo de bitácoras."""

import json
import logging
from src.core.logger import get_system_logger, JSONFormatter, ColoredConsoleFormatter

def test_json_formatter_structure() -> None:
    """Garantiza la inyección correcta de campos para observabilidad."""
    formatter = JSONFormatter()
    record = logging.LogRecord("test_logger", logging.INFO, "module.py", 10, "Test message", None, None)
    formatted = formatter.format(record)
    parsed = json.loads(formatted)
    
    assert "timestamp" in parsed
    assert parsed["level"] == "INFO"
    assert parsed["message"] == "Test message"
    assert parsed["module"] == "module"

def test_colored_console_formatter() -> None:
    """Verifica la asignación de colores ANSI según el nivel."""
    formatter = ColoredConsoleFormatter()
    record = logging.LogRecord("test", logging.ERROR, "mod", 1, "Error occurred", None, None)
    formatted = formatter.format(record)
    
    assert "Error occurred" in formatted
    assert "\033[31m" in formatted  # Código ANSI para Rojo (ERROR)

def test_logger_singleton_handlers() -> None:
    """Evita la duplicación de flujos (handlers) en la consola."""
    logger1 = get_system_logger("test_singleton")
    handlers_count = len(logger1.handlers)
    
    logger2 = get_system_logger("test_singleton")
    assert len(logger2.handlers) == handlers_count