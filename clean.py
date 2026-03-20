"""Script de limpieza transaccional del entorno de trabajo.

Provee un mecanismo seguro para purgar directorios temporales y cachés
de ejecuciones previas, garantizando la inmutabilidad de la memoria
histórica (contexto de la IA).
"""

import shutil
import sys
from pathlib import Path

from src.config import get_settings
from src.core.logger import get_system_logger

logger = get_system_logger(__name__)
settings = get_settings()


def safe_clean_directory(directory: Path) -> None:
    """Elimina recursivamente un directorio y lo recrea vacío de forma segura.

    Args:
        directory: Objeto Path que apunta al directorio objetivo.
    """
    if directory.exists():
        try:
            shutil.rmtree(directory)
            logger.debug(f"Directorio purgado exitosamente: {directory.name}/")
        except PermissionError as e:
            logger.error(f"Fallo de permisos al intentar limpiar {directory.name}/. ¿Archivo en uso?: {e}")
            return
        except OSError as e:
            logger.error(f"Error de sistema operativo al acceder a {directory.name}/: {e}")
            return

    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Imposible recrear la estructura de directorio {directory.name}/: {e}")


def execute_cleanup() -> None:
    """Ejecuta la rutina de purga de áreas de stage protegiendo la persistencia de IA."""
    logger.info("Iniciando secuencia de limpieza del entorno de procesamiento.")

    directories_to_clean = [
        settings.explosion_dir,
        settings.final_dir
    ]

    for directory in directories_to_clean:
        safe_clean_directory(directory)

    # Verificación explícita de la memoria histórica
    context_file = settings.data_dir / "system_context.json"
    if context_file.exists():
        logger.info(f"Memoria histórica detectada y protegida: {context_file.name} conservado para aprendizaje continuo.")
    else:
        logger.info("Memoria histórica no detectada. Se inicializará un nuevo catálogo en la próxima ejecución.")

    logger.info("Rutina de limpieza finalizada. Entorno transaccional listo.")


if __name__ == "__main__":
    try:
        execute_cleanup()
    except KeyboardInterrupt:
        logger.warning("Rutina de limpieza interrumpida por el usuario.")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Fallo catastrófico no controlado en la rutina de limpieza: {e}", exc_info=True)
        sys.exit(1)