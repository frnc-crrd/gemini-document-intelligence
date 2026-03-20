import logging
import os
import subprocess
import zipfile
from pathlib import Path
from typing import Final, Optional, Set

# Configuración de logging estructurado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ZipExtractionError(Exception):
    """Excepción de dominio para errores en el proceso de descompresión."""
    pass


class AppConfig:
    """Configuración centralizada de la aplicación."""
    TARGET_DIR: Final[Path] = Path(
        "/home/frnc/Software/gemini-document-intelligence/data/01_raws"
    )
    # Patrones de archivos que no aportan valor al procesamiento de documentos
    IGNORABLE_PATTERNS: Final[Set[str]] = {
        "thumbs.db",
        ".ds_store",
        "desktop.ini",
        "__macosx",
        "metadata"
    }


class ZenityProvider:
    """Proveedor de servicios de interfaz de usuario nativa para GNOME/Wayland."""

    @staticmethod
    def select_file(title: str, file_filter: str) -> Optional[Path]:
        """
        Invoca el portal de selección de archivos del sistema operativo.
        
        :param title: Título de la ventana de diálogo.
        :param file_filter: Filtro de extensión de archivos.
        :return: Path del archivo seleccionado o None si se cancela.
        """
        try:
            cmd = [
                "zenity",
                "--file-selection",
                f"--title={title}",
                f"--file-filter={file_filter}"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return Path(result.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    @staticmethod
    def notify(title: str, text: str, error: bool = False) -> None:
        """Muestra una notificación nativa al usuario."""
        dialog = "--error" if error else "--info"
        subprocess.run(["zenity", dialog, f"--title={title}", f"--text={text}"])


class DocumentIngestionProcessor:
    """
    Clase responsable de la lógica de negocio para la extracción y 
    normalización de documentos desde contenedores ZIP.
    """

    def __init__(self, source_path: Path, target_path: Path) -> None:
        """
        Inicializa el procesador con las rutas de origen y destino.
        
        :param source_path: Ruta al archivo ZIP.
        :param target_path: Ruta al directorio de salida.
        """
        self.source_path: Final[Path] = source_path
        self.target_path: Final[Path] = target_path

    def _should_ignore(self, filename: str) -> bool:
        """Aplica lógica de filtrado para archivos de sistema."""
        name_lower = filename.lower()
        return any(p in name_lower for p in AppConfig.IGNORABLE_PATTERNS)

    def _generate_unique_path(self, base_path: Path) -> Path:
        """Resuelve colisiones de nombres mediante sufijos incrementales."""
        counter: int = 1
        unique_path: Path = base_path
        while unique_path.exists():
            unique_path = base_path.with_name(
                f"{base_path.stem}_{counter}{base_path.suffix}"
            )
            counter += 1
        return unique_path

    def process(self) -> None:
        """
        Ejecuta la extracción, aplanado y limpieza.
        
        :raises ZipExtractionError: Si el archivo ZIP es inválido o inaccesible.
        """
        if not self.target_path.exists():
            self.target_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directorio de destino creado en: {self.target_path}")

        try:
            with zipfile.ZipFile(self.source_path, 'r') as zip_ref:
                for member in zip_ref.infolist():
                    if member.is_dir():
                        continue

                    # Extraer solo el nombre base para aplanar la estructura
                    original_filename = os.path.basename(member.filename)
                    
                    if not original_filename or self._should_ignore(original_filename):
                        continue

                    dest_file_path = self.target_path / original_filename
                    final_path = self._generate_unique_path(dest_file_path)

                    # Extracción directa del stream para optimizar memoria
                    with zip_ref.open(member) as source, open(final_path, 'wb') as target:
                        target.write(source.read())
            
            logger.info(f"Ingesta completada: {self.source_path.name}")
            
        except zipfile.BadZipFile as exc:
            raise ZipExtractionError("El archivo ZIP está corrupto.") from exc
        except Exception as exc:
            raise ZipExtractionError(f"Fallo crítico en el procesamiento: {exc}") from exc


def main() -> None:
    """Punto de entrada principal de la utilidad."""
    ui = ZenityProvider()
    
    selected_zip = ui.select_file(
        "Seleccione el ZIP de documentos para procesar",
        "Archivos ZIP | *.zip *.ZIP"
    )

    if not selected_zip:
        logger.info("Operación cancelada por el usuario.")
        return

    try:
        processor = DocumentIngestionProcessor(selected_zip, AppConfig.TARGET_DIR)
        processor.process()
        ui.notify("Éxito", "Documentos extraídos y normalizados correctamente.")
    except Exception as exc:
        logger.error(f"Error: {exc}")
        ui.notify("Error", str(exc), error=True)


if __name__ == "__main__":
    main()