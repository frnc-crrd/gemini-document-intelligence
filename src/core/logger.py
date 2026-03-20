"""Módulo central de observabilidad y registro estructurado.

Provee un mecanismo unificado y dual para la emisión de bitácoras del sistema.
Implementa un formateador minimalista con color para la salida estándar, y un
formateador JSON estricto dirigido a almacenamiento físico para auditoría y
análisis posterior utilizando el formato JSON Lines (.jsonl).
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class LogColors:
    """Definición de secuencias de escape ANSI para colorización en terminales UNIX."""
    RESET = "\033[0m"
    DEBUG = "\033[36m"     # Cyan
    INFO = "\033[32m"      # Verde
    WARNING = "\033[33m"   # Amarillo
    ERROR = "\033[31m"     # Rojo
    CRITICAL = "\033[1;31m" # Rojo negrita


class ColoredConsoleFormatter(logging.Formatter):
    """Formateador minimalista con inyección de color para salida estándar."""

    def format(self, record: logging.LogRecord) -> str:
        """Transforma el registro en una cadena plana colorizada."""
        color = getattr(LogColors, record.levelname, LogColors.RESET)
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        message = record.getMessage()
        return f"{color}{timestamp} - {record.levelname} - {message}{LogColors.RESET}"


class JSONFormatter(logging.Formatter):
    """Formateador de bitácoras que serializa los registros a formato JSON estricto."""

    def format(self, record: logging.LogRecord) -> str:
        """Transforma un objeto LogRecord en una cadena JSON."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


def get_system_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Instancia y configura un logger con flujo dual (Consola y Archivo).

    Garantiza la creación del directorio de auditoría físico y previene
    la duplicación de manejadores en llamadas subsecuentes.

    Args:
        name: Identificador del componente que emite el log.
        level: Nivel mínimo de severidad a registrar. Por defecto INFO.

    Returns:
        logging.Logger: Instancia configurada para flujo dual.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColoredConsoleFormatter())
        logger.addHandler(console_handler)

        log_dir = Path(__file__).resolve().parent.parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Modificación: Transición a .jsonl para evitar errores de sintaxis en analizadores estáticos
        file_handler = logging.FileHandler(log_dir / "audit_trail.jsonl", encoding="utf-8")
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
        
        logger.propagate = False

    return logger