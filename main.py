"""Punto de entrada principal para el Sistema de Auditoría Documental CxC.

Orquesta la inicialización del esquema transaccional, desencadena el flujo de 
procesamiento concurrente segmentado, persiste los artefactos en PostgreSQL
y actualiza progresivamente el respaldo tabular en Excel.
"""

import sys

from src.core.processor import PipelineProcessor
from src.core.analyzer import DocumentAnalyzer
from src.utils.report_generator import ReportGenerator
from src.db.repository import PostgresRepository
from src.core.logger import get_system_logger

logger = get_system_logger("main")


def execute_audit_pipeline() -> None:
    """Coordina la ejecución end-to-end garantizando salidas limpias e Inyección de Dependencias."""
    logger.info("SISTEMA DE AUDITORÍA DOCUMENTAL CXC: Inicializando.")
    
    try:
        repo = PostgresRepository()
        repo.initialize_schema()
    except Exception as e:
        logger.critical(f"Abortando ejecución. La base de datos no está disponible: {e}")
        sys.exit(1)
    
    analyzer = DocumentAnalyzer()
    processor = PipelineProcessor(analyzer=analyzer, db_repo=repo)
    
    resultados_totales = []
    lotes_procesados = 0

    # Consumimos el generador: Cada iteración procesa un chunk físico
    for resultados_lote in processor.run():
        lotes_procesados += 1
        
        if resultados_lote:
            logger.info("Persistiendo resultados del lote actual en PostgreSQL...")
            try:
                # El bloque try asegura que si la BD rechaza el lote, el script finalice 
                # antes de que el generador se reanude y mueva los archivos a /04_processed
                repo.upsert_batch(resultados_lote)
            except Exception as e:
                logger.critical(f"Fallo crítico de persistencia en lote {lotes_procesados}. Ejecución abortada para prevenir desincronización física: {e}")
                sys.exit(1)
            
            resultados_totales.extend(resultados_lote)
            
            logger.info("Actualizando artefacto secundario de auditoría (Excel)...")
            ReportGenerator.generate_excel(resultados_totales)
        else:
            logger.info("El lote actual no generó entidades extraíbles válidas.")

    if resultados_totales or lotes_procesados > 0:
        logger.info(f"Ejecución masiva finalizada. Lotes procesados: {lotes_procesados}.")
    else:
        logger.warning("El pipeline concluyó sin encontrar archivos físicos válidos en el directorio origen.")


if __name__ == "__main__":
    try:
        execute_audit_pipeline()
    except KeyboardInterrupt:
        logger.warning("Proceso interrumpido manualmente por el usuario (SIGINT).")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as e:
        logger.critical(f"Fallo irrecuperable en el hilo principal: {e}", exc_info=True)
        sys.exit(1)