"""Punto de entrada principal para el Sistema de Auditoría Documental CxC.

Orquesta la inicialización del esquema transaccional, desencadena el flujo de 
procesamiento concurrente Map-Reduce, persiste los artefactos en PostgreSQL
y genera el respaldo tabular en Excel.
"""

import sys

from src.core.processor import PipelineProcessor
from src.utils.report_generator import ReportGenerator
from src.db.repository import PostgresRepository
from src.core.logger import get_system_logger

logger = get_system_logger("main")


def execute_audit_pipeline() -> None:
    """Coordina la ejecución end-to-end garantizando salidas limpias e Inyección de Dependencias."""
    logger.info("SISTEMA DE AUDITORÍA DOCUMENTAL CXC: Inicializando.")
    
    try:
        # 1. Validación de Infraestructura de Base de Datos
        repo = PostgresRepository()
        repo.initialize_schema()
    except Exception as e:
        logger.critical(f"Abortando ejecución. La base de datos no está disponible: {e}")
        sys.exit(1)
    
    # 2. Ejecutar Arquitectura Map-Reduce (Inyectando el repositorio para reglas DDL)
    processor = PipelineProcessor(db_repo=repo)
    resultados = processor.run()
    
    if resultados:
        # 3. Persistencia Transaccional (UPSERT para deduplicación)
        logger.info("Iniciando UPSERT masivo de metadatos en PostgreSQL...")
        repo.upsert_batch(resultados)
        
        # 4. Respaldo Tabular Físico
        logger.info("Generando artefacto secundario de auditoría (Excel)...")
        ReportGenerator.generate_excel(resultados)
        
        logger.info("Ejecución del pipeline transaccional finalizada con éxito.")
    else:
        logger.warning("El pipeline concluyó sin entidades lógicas válidas para procesar.")


if __name__ == "__main__":
    try:
        execute_audit_pipeline()
    except KeyboardInterrupt:
        logger.warning("Proceso interrumpido manualmente por el usuario (SIGINT).")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Abrazo mortal: Fallo irrecuperable en el hilo principal: {e}", exc_info=True)
        sys.exit(1)