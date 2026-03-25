"""Generador de reportes de auditoría y trazabilidad documental.

Transforma los resultados estructurados del pipeline en artefactos tabulares (Excel).
Implementa un patrón de estado (Stateful) para mantener un único archivo de salida
por ejecución, renombrándolo dinámicamente con el último timestamp.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

from src.config import get_settings
from src.core.logger import get_system_logger

logger = get_system_logger(__name__)
settings = get_settings()

# Ajuste de Zona Horaria a Gómez Palacio, Durango
tz_local = ZoneInfo("America/Monterrey")


class ReportGenerator:
    """Clase utilitaria para la exportación de resultados transaccionales."""
    
    _last_report_path: Optional[Path] = None

    @classmethod
    def generate_excel(cls, processed_data: List[Dict[str, Any]]) -> Optional[Path]:
        """Genera un archivo Excel con el log de la auditoría y trazabilidad."""
        if not processed_data:
            logger.warning("Solicitud de reporte denegada: No hay datos procesados en el lote actual.")
            return None
            
        try:
            df = pd.DataFrame(processed_data)
            
            columns_order = [
                "Folio", 
                "Categoría", 
                "Cliente", 
                "Archivo Original",
                "Tipo Original",
                "Páginas Original",
                "Páginas Final",
                "Status", 
                "Confianza", 
                "Ruta del Archivo", 
                "Justificación"
            ]
            
            missing_cols = set(columns_order) - set(df.columns)
            if missing_cols:
                for col in missing_cols:
                    df[col] = "N/A"
            
            df = df[columns_order]
            
            timestamp = datetime.now(tz_local).strftime("%Y%m%d_%H%M%S")
            report_filename = f"Auditoria_Integridad_{timestamp}.xlsx"
            report_path = settings.data_dir / report_filename
            
            df.to_excel(report_path, index=False, sheet_name="Auditoría")
            
            if cls._last_report_path and cls._last_report_path.exists():
                try:
                    cls._last_report_path.unlink()
                except OSError as e:
                    logger.warning(f"No se pudo limpiar el reporte intermedio anterior: {e}")
            
            cls._last_report_path = report_path
            
            logger.info(f"Reporte transaccional consolidado exitosamente: {report_path.relative_to(settings.base_dir)}")
            return report_path
            
        except PermissionError as e:
            logger.error(
                f"Bloqueo de I/O detectado. Cierre el archivo Excel si lo tiene abierto para permitir la actualización: {e}", 
                exc_info=True
            )
            return None
        except ImportError as e:
            logger.error(f"Dependencia faltante para exportación. Asegúrese de instalar 'openpyxl': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Fallo crítico inesperado al generar el artefacto tabular: {e}", exc_info=True)
            return None