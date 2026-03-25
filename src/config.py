"""Configuración central del sistema de Auditoría CxC.

Implementa validación estricta de variables de entorno utilizando Pydantic.
Garantiza que el sistema falle de manera temprana si las credenciales o
configuraciones críticas no están presentes en el entorno de ejecución.
Aplica patrón Singleton mediante caché para evitar sobrecarga de I/O.
"""

from functools import lru_cache
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Definición estricta y tipada de la configuración del sistema."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    gemini_api_key: str = Field(..., description="Clave de API obligatoria para Google Gemini.")
    gemini_model: str = Field(default="gemini-2.5-flash", description="Modelo de procesamiento primario.")

    database_url: str = Field(
        default="postgresql://ai_invoice_admin:secure_password_123@localhost:5432/ai_invoice_db",
        description="Cadena de conexión para el motor PostgreSQL."
    )

    execution_mode: str = Field(default="local", description="Define el comportamiento del almacenamiento (local/cloud).")
    aws_bucket_name: str = Field(default="tu-bucket-produccion", description="Bucket S3 destino cuando execution_mode='cloud'.")

    dpi_conversion: int = Field(default=200, ge=72, le=600, description="Resolución de rasterización (DPI).")
    max_retries: int = Field(default=3, ge=1, description="Límite máximo de reintentos para peticiones externas.")
    api_delay: float = Field(default=0.5, ge=0.1, description="Segundos base para pausas entre peticiones.")
    
    max_threads: int = Field(default=3, ge=1, description="Número de hilos concurrentes reducido para evitar saturación de API.")
    vision_batch_size: int = Field(default=15, ge=1, description="Cantidad máxima de imágenes a enviar por petición a la API.")
    
    pdf_chunk_max_pages: int = Field(default=100, ge=10, description="Límite máximo de páginas acumuladas por lote de PDFs.")
    physical_chunk_size: int = Field(default=50, ge=1, description="Límite máximo de imágenes sueltas a empaquetar por lote.")

    error_no_detectado: str = Field(default="ERROR_SIN_FOLIO")
    error_ilegible: str = Field(default="ERROR_DOCUMENTO_ILEGIBLE")

    @property
    def base_dir(self) -> Path:
        return Path(__file__).resolve().parent.parent

    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "01_raw"

    @property
    def explosion_dir(self) -> Path:
        return self.data_dir / "02_explosion"

    @property
    def final_dir(self) -> Path:
        return self.data_dir / "03_final"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "04_processed"


@lru_cache()
def get_settings() -> Settings:
    """Instancia y retorna la configuración validada del sistema (Singleton)."""
    return Settings()