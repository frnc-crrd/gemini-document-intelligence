"""Script de extracción y aplanado de documentos origen.

Procesa archivos comprimidos (ZIP) depositados en el entorno, extrayendo
su contenido ignorando las estructuras de subdirectorios para conformar
el Data Lake inicial (01_raw). Aplica resolución de colisiones y omite
archivos irrelevantes del sistema operativo.
"""

import zipfile
import shutil
from pathlib import Path
from typing import Final, List, Set, Tuple
import uuid

from src.config import get_settings
from src.core.logger import get_system_logger

logger = get_system_logger(__name__)
settings = get_settings()

class AppConfig:
    """Configuración dinámica unificada con el orquestador principal."""
    TARGET_DIR: Final[Path] = settings.raw_dir
    ALLOWED_EXTENSIONS: Final[Set[str]] = {'.pdf', '.jpg', '.jpeg', '.png'}
    IGNORED_PREFIXES: Final[Tuple[str, ...]] = ('__MACOSX', '.', '~')


class ZipExtractionError(Exception):
    """Excepción específica para fallos controlados en el proceso de extracción."""
    pass


class DocumentIngestionProcessor:
    """Manejador de operaciones de I/O sobre empaquetados ZIP."""

    def __init__(self, source_path: Path, target_path: Path) -> None:
        self.source_path = source_path
        self.target_path = target_path

    def _should_ignore(self, filename: str) -> bool:
        """Determina si un archivo debe ser ignorado por reglas de negocio."""
        if any(filename.startswith(prefix) for prefix in AppConfig.IGNORED_PREFIXES):
            return True
        if Path(filename).suffix.lower() not in AppConfig.ALLOWED_EXTENSIONS:
            return True
        return False

    def _generate_unique_path(self, original_path: Path) -> Path:
        """Resuelve colisiones de nomenclatura anexando entropía criptográfica."""
        if not original_path.exists():
            return original_path
            
        stem = original_path.stem
        suffix = original_path.suffix
        new_name = f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
        return original_path.with_name(new_name)

    def process(self) -> None:
        """Ejecuta el pipeline de extracción y aplanado topológico."""
        self.target_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Iniciando ingesta transaccional desde: {self.source_path.name}")
        
        try:
            with zipfile.ZipFile(self.source_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.endswith('/'):
                        continue
                        
                    original_filename = Path(member).name
                    if not original_filename or self._should_ignore(original_filename):
                        continue

                    dest_file_path = self.target_path / original_filename
                    final_path = self._generate_unique_path(dest_file_path)

                    with zip_ref.open(member) as source, open(final_path, 'wb') as target:
                        target.write(source.read())
            
            logger.info(f"Ingesta completada y normalizada en: {self.target_path}")
            
        except zipfile.BadZipFile as exc:
            logger.error("El archivo ZIP se encuentra corrupto o tiene un formato no soportado.")
            raise ZipExtractionError("El archivo ZIP está corrupto.") from exc
        except Exception as exc:
            logger.error(f"Violación de I/O durante la extracción: {exc}", exc_info=True)
            raise ZipExtractionError(f"Fallo crítico en el procesamiento: {exc}") from exc


def main() -> None:
    """Punto de entrada de línea de comandos para la utilidad."""
    import argparse
    parser = argparse.ArgumentParser(description="Utilidad de Ingesta Documental CxC")
    parser.add_argument("zip_path", type=Path, help="Ruta absoluta o relativa al archivo ZIP origen.")
    args = parser.parse_args()

    if not args.zip_path.exists() or not args.zip_path.is_file():
        logger.critical(f"El archivo origen no existe o es inaccesible: {args.zip_path}")
        return

    try:
        processor = DocumentIngestionProcessor(args.zip_path, AppConfig.TARGET_DIR)
        processor.process()
    except Exception as exc:
        logger.critical(f"La utilidad de ingesta abortó su ejecución: {exc}")


if __name__ == "__main__":
    main()