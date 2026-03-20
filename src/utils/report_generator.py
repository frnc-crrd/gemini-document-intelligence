"""Generador de reportes de auditoría y trazabilidad documental.

Transforma los resultados estructurados del pipeline en artefactos tabulares (Excel).
Garantiza el manejo de excepciones de I/O, previniendo fallos por bloqueos de
archivos a nivel de sistema operativo y asegurando trazabilidad vía logs.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from src.config import get_settings
from src.core.logger import get_system_logger

logger = get_system_logger(__name__)
settings = get_settings()


class ReportGenerator:
    """Clase utilitaria estática para la exportación de resultados transaccionales."""

    @staticmethod
    def generate_excel(processed_data: List[Dict[str, Any]]) -> Optional[Path]:
        """Genera un archivo Excel con el log de la auditoría y trazabilidad.

        Args:
            processed_data: Lista de diccionarios con la metadata de cada documento.

        Returns:
            Path: Ruta del archivo Excel generado, o None si ocurre un fallo.
        """
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
            
            # Asegurar que todas las columnas existan en el DataFrame antes del filtrado
            missing_cols = set(columns_order) - set(df.columns)
            if missing_cols:
                logger.warning(f"Faltan columnas esperadas en los resultados: {missing_cols}")
                for col in missing_cols:
                    df[col] = "N/A"
            
            df = df[columns_order]
            
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            report_filename = f"Auditoria_Integridad_{timestamp}.xlsx"
            report_path = settings.data_dir / report_filename
            
            df.to_excel(report_path, index=False, sheet_name="Auditoría")
            
            logger.info(f"Reporte transaccional generado exitosamente: {report_path.relative_to(settings.base_dir)}")
            return report_path
            
        except PermissionError as e:
            logger.error(
                f"Bloqueo de I/O detectado. Cierre el archivo si está abierto en Excel u otro programa: {e}", 
                exc_info=True
            )
            return None
        except ImportError as e:
            logger.error(
                f"Dependencia faltante para exportación. Asegúrese de instalar 'openpyxl': {e}", 
                exc_info=True
            )
            return None
        except Exception as e:
            logger.error(f"Fallo crítico inesperado al generar el artefacto tabular: {e}", exc_info=True)
            return None