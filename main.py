"""Punto de entrada principal para la aplicación de Integridad Documental."""

from src.core.processor import PipelineProcessor
from src.utils.report_generator import ReportGenerator

def main():
    print("=" * 50)
    print("SISTEMA DE GESTIÓN DE INTEGRIDAD DOCUMENTAL")
    print("=" * 50)
    
    # Iniciar y correr el pipeline
    processor = PipelineProcessor()
    resultados = processor.run()
    
    # Generar el reporte de auditoría si hubo documentos procesados
    if resultados:
        print("\nGenerando reporte de auditoría...")
        ReportGenerator.generate_excel(resultados)

if __name__ == "__main__":
    main()