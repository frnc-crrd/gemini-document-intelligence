"""Módulo central de observabilidad y registro estructurado.

Provee un mecanismo unificado y dual para la emisión de bitácoras del sistema.
Implementa un formateador minimalista con color para la salida estándar, y un
formateador JSON estricto dirigido a almacenamiento físico para auditoría.
"""

import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict

# Ajuste de Zona Horaria a Gómez Palacio, Durango (CST/CDT)
tz_local = ZoneInfo("America/Monterrey")

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
        timestamp = datetime.fromtimestamp(record.created, tz=tz_local).strftime("%Y-%m-%d %H:%M:%S")
        message = record.getMessage()
        return f"{color}{timestamp} - {record.levelname} - {message}{LogColors.RESET}"


class JSONFormatter(logging.Formatter):
    """Formateador de bitácoras que serializa los registros a formato JSON estricto."""

    def format(self, record: logging.LogRecord) -> str:
        """Transforma un objeto LogRecord en una cadena JSON."""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=tz_local).isoformat(),
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
    """Instancia y configura un logger con flujo dual (Consola y Archivo)."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColoredConsoleFormatter())
        logger.addHandler(console_handler)

        log_dir = Path(__file__).resolve().parent.parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_dir / "audit_trail.jsonl", encoding="utf-8")
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
        
        logger.propagate = False

    return logger