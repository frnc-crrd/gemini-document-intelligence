"""Punto de entrada principal para el Sistema de Gestión de Integridad Documental.

Desencadena el flujo de procesamiento masivo coordinando el orquestador
y la capa de presentación. Implementa un bloque `try/except` global para
garantizar salidas limpias ante fallos no recuperables en el ciclo de vida.
"""

import sys

from src.core.processor import PipelineProcessor
from src.utils.report_generator import ReportGenerator
from src.core.logger import get_system_logger

logger = get_system_logger("main")


def execute_audit_pipeline() -> None:
    """Coordina la inicialización de dependencias y ejecución del orquestador."""
    logger.info("SISTEMA DE GESTIÓN DE INTEGRIDAD DOCUMENTAL: Inicializando.")
    
    processor = PipelineProcessor()
    resultados = processor.run()
    
    if resultados:
        logger.info("Generando artefacto final de auditoría...")
        ReportGenerator.generate_excel(resultados)
        logger.info("Ejecución del pipeline finalizada con éxito.")
    else:
        logger.warning("El pipeline concluyó sin entidades lógicas válidas para reportar.")


if __name__ == "__main__":
    try:
        execute_audit_pipeline()
    except KeyboardInterrupt:
        logger.warning("Proceso interrumpido manualmente por el usuario (SIGINT).")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Abrazo mortal: Fallo irrecuperable en el hilo principal: {e}", exc_info=True)
        sys.exit(1)