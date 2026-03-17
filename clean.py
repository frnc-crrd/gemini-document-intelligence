"""Script de limpieza del entorno de trabajo.

Elimina los archivos generados en ejecuciones previas dentro de los 
directorios de explosión y resultados finales. 
Protege y conserva el archivo de contexto (system_context.json) para 
permitir el aprendizaje continuo y la evolución orgánica de la IA.
"""

import shutil
from pathlib import Path
from src import config

def clean_directory(directory: Path) -> None:
    """Elimina recursivamente un directorio y lo vuelve a crear vacío.

    Args:
        directory: Objeto Path que apunta al directorio a limpiar.
    """
    if directory.exists():
        try:
            shutil.rmtree(directory)
            print(f"Directorio limpiado: {directory.name}/")
        except Exception as e:
            print(f"Error al limpiar {directory.name}/: {str(e)}")
            return

    directory.mkdir(parents=True, exist_ok=True)

def main():
    """Ejecuta la rutina de limpieza protegiendo la memoria histórica."""
    print("=" * 50)
    print("LIMPIEZA DE ENTORNO DE PROCESAMIENTO")
    print("=" * 50)

    directories_to_clean = [
        config.EXPLOSION_DIR,
        config.FINAL_DIR
    ]

    for directory in directories_to_clean:
        clean_directory(directory)

    context_file = config.DATA_DIR / "system_context.json"
    if context_file.exists():
        print(f"Memoria histórica protegida: {context_file.name} conservado para aprendizaje continuo.")
    else:
        print("No se encontró memoria histórica. Se creará una nueva en la próxima ejecución.")

    print("-" * 50)
    print("Entorno listo. Puedes ejecutar main.py sin perder el conocimiento adquirido.")
    print("=" * 50)

if __name__ == "__main__":
    main()