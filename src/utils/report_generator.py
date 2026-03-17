"""Generador de reportes de auditoría en Excel.

Toma los resultados estructurados del procesamiento masivo y genera
un archivo tabulado ideal para revisión humana, importación a bases de datos
y validación de trazabilidad de páginas.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from src import config

class ReportGenerator:
    """Clase utilitaria para la exportación de resultados."""

    @staticmethod
    def generate_excel(processed_data: list) -> Path:
        """Genera un archivo Excel con el log de la auditoría y trazabilidad.

        Args:
            processed_data: Lista de diccionarios con la información de cada documento.

        Returns:
            Ruta (Path) del archivo Excel generado.
        """
        if not processed_data:
            print("  [!] No hay datos procesados para generar el reporte.")
            return None
            
        # Convertir la lista de diccionarios a un DataFrame de Pandas
        df = pd.DataFrame(processed_data)
        
        # Ordenar las columnas para asegurar trazabilidad visual
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
        
        # Filtrar solo las columnas deseadas en el orden correcto
        df = df[columns_order]
        
        # Crear nombre de archivo con timestamp para no sobrescribir reportes anteriores
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"Auditoria_Integridad_{timestamp}.xlsx"
        report_path = config.DATA_DIR / report_filename
        
        # Exportar a Excel
        try:
            df.to_excel(report_path, index=False, sheet_name="Auditoría")
            print(f"\n[+] Reporte Excel generado exitosamente en: {report_path.relative_to(config.BASE_DIR)}")
            return report_path
        except Exception as e:
            print(f"\n[X] Error al generar el Excel: {str(e)}")
            print("    Asegúrate de tener instalada la librería openpyxl (pip install openpyxl)")
            return None