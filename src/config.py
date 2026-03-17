"""Configuración central del proyecto.

Maneja variables de entorno, rutas de trabajo, configuraciones de la API de Gemini
y catálogos estáticos del sistema.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# API KEYS Y MODELO
# ============================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"

# ============================================================
# MODO DE EJECUCIÓN (LOCAL vs CLOUD)
# ============================================================
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "local").lower()
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "tu-bucket-produccion")

# ============================================================
# RUTAS DE TRABAJO
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "01_raw"
EXPLOSION_DIR = DATA_DIR / "02_explosion"
FINAL_DIR = DATA_DIR / "03_final"

# ============================================================
# CONFIGURACIÓN DE PROCESAMIENTO
# ============================================================
DPI_CONVERSION = 200
MAX_RETRIES = 3
API_DELAY = 0.5

# Categorías de error estándar
ERROR_NO_DETECTADO = "ERROR_SIN_FOLIO"
ERROR_ILEGIBLE = "ERROR_DOCUMENTO_ILEGIBLE"