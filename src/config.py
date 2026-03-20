"""Configuración central del sistema de Auditoría CxC.

Implementa validación estricta de variables de entorno utilizando Pydantic.
Garantiza que el sistema falle de manera temprana si las credenciales o
configuraciones críticas no están presentes en el entorno de ejecución.
"""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Definición estricta y tipada de la configuración del sistema.
    
    Attributes:
        gemini_api_key: Credencial requerida para el consumo del modelo.
        gemini_model: Identificador del modelo fundacional a utilizar.
        execution_mode: Entorno de despliegue ('local' o 'cloud').
        aws_bucket_name: Identificador del bucket para almacenamiento remoto.
        dpi_conversion: Resolución estándar para la rasterización de documentos.
        max_retries: Límite de intentos para tolerancia a fallos de red.
        api_delay: Tiempo de espera base para el backoff exponencial.
        error_no_detectado: Etiqueta estándar para documentos sin folio.
        error_ilegible: Etiqueta estándar para documentos no procesables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Autenticación y Modelos
    gemini_api_key: str = Field(
        ..., 
        description="Clave de API obligatoria para Google Gemini."
    )
    gemini_model: str = Field(
        default="gemini-2.5-flash", 
        description="Modelo de procesamiento primario."
    )

    # Entorno de Ejecución
    execution_mode: str = Field(
        default="local", 
        description="Define el comportamiento del almacenamiento (local/cloud)."
    )
    aws_bucket_name: str = Field(
        default="tu-bucket-produccion", 
        description="Bucket S3 destino cuando execution_mode='cloud'."
    )

    # Parámetros de Procesamiento
    dpi_conversion: int = Field(
        default=200, 
        ge=72, 
        le=600, 
        description="Resolución de rasterización (DPI)."
    )
    max_retries: int = Field(
        default=3, 
        ge=1, 
        description="Límite máximo de reintentos para peticiones externas."
    )
    api_delay: float = Field(
        default=0.5, 
        ge=0.1, 
        description="Segundos base para pausas entre peticiones."
    )

    # Constantes de Estado
    error_no_detectado: str = Field(default="ERROR_SIN_FOLIO")
    error_ilegible: str = Field(default="ERROR_DOCUMENTO_ILEGIBLE")

    @property
    def base_dir(self) -> Path:
        """Calcula el directorio raíz del proyecto dinámicamente.
        
        Returns:
            Path: Ruta absoluta a la raíz del proyecto.
        """
        # Resolviendo: src/config.py -> src/ -> raiz/
        return Path(__file__).resolve().parent.parent

    @property
    def data_dir(self) -> Path:
        """Ruta al directorio de persistencia de datos local."""
        return self.base_dir / "data"

    @property
    def raw_dir(self) -> Path:
        """Ruta al directorio de ingesta de documentos crudos."""
        return self.data_dir / "01_raw"

    @property
    def explosion_dir(self) -> Path:
        """Ruta al directorio de almacenamiento temporal por página."""
        return self.data_dir / "02_explosion"

    @property
    def final_dir(self) -> Path:
        """Ruta al directorio de documentos procesados y unificados."""
        return self.data_dir / "03_final"


# Se retrasa la instanciación para permitir la carga de variables
# en el entorno de pruebas antes de evaluar las validaciones.
def get_settings() -> Settings:
    """Instancia y retorna la configuración validada del sistema.
    
    Returns:
        Settings: Objeto con la configuración validada.
    """
    return Settings()