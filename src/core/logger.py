"""Módulo central de observabilidad y registro estructurado.

Provee un mecanismo unificado para la emisión de bitácoras del sistema.
Implementa un formateador JSON personalizado para garantizar que los
eventos sean ingestables por plataformas de monitoreo como AWS CloudWatch,
Datadog o ELK, cumpliendo con los estándares de despliegue continuo.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Formateador de bitácoras que serializa los registros a formato JSON estricto."""

    def format(self, record: logging.LogRecord) -> str:
        """Transforma un objeto LogRecord en una cadena JSON.

        Args:
            record: Objeto interno de logging que representa un evento.

        Returns:
            str: Cadena de texto en formato JSON conteniendo los datos del evento.
        """
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Se inyecta la traza de la pila si existe una excepción asociada
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


def get_system_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Instancia y configura un logger estructurado.

    Garantiza que el logger utilice la salida estándar (stdout) y previene
    la duplicación de manejadores en llamadas subsecuentes al mismo identificador.

    Args:
        name: Identificador del componente que emite el log (típicamente __name__).
        level: Nivel mínimo de severidad a registrar. Por defecto INFO.

    Returns:
        logging.Logger: Instancia configurada y lista para emitir eventos.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # El condicional evita asignar múltiples manejadores en llamadas repetidas
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        
        # Prevenir propagación al logger raíz para no duplicar salidas
        logger.propagate = False

    return logger