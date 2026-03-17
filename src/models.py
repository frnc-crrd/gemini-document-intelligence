"""Modelos de datos para el sistema de integridad documental.

Implementa estructuras para un pipeline Multi-Agente:
1. Esquemas de orientación (Agente Espacial - Anclaje de Lectura).
2. Esquemas de extracción lógica (Agente Auditor).
3. Estructura unificada para el orquestador.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

# ==========================================
# ESQUEMAS: AGENTE 1 (ESPACIAL)
# ==========================================
class PageOrientation(BaseModel):
    file_name: str = Field(..., description="Nombre del archivo.")
    orientacion: str = Field(
        ..., 
        description="Dirección de lectura: 'NORMAL', 'ACOSTADO_IZQUIERDA', 'ACOSTADO_DERECHA', 'INVERTIDO'."
    )

class OrientationResponse(BaseModel):
    orientations: List[PageOrientation]

# ==========================================
# ESQUEMAS: AGENTE 2 (AUDITOR)
# ==========================================
class LogicalDocExtraction(BaseModel):
    folios: List[str] = Field(..., description="Lista de folios. Si es un diario de ventas, incluye todos.")
    ordered_file_names: List[str] = Field(..., description="Nombres de archivo en orden de lectura (SAT al final).")
    document_type: str = Field(..., description="Categoría del documento.")
    client_name: Optional[str] = Field(None, description="Nombre del cliente.")
    confidence_score: int = Field(..., description="Confianza (0-100).")
    reasoning: str = Field(..., description="Justificación de la extracción.")

class ExtractionResponse(BaseModel):
    documents: List[LogicalDocExtraction]

# ==========================================
# ESQUEMA FINAL (PARA EL PROCESSOR)
# ==========================================
class PageInstruction(BaseModel):
    file_name: str
    rotation_degrees: int

class LogicalDocument(BaseModel):
    folios: List[str]
    pages: List[PageInstruction]
    document_type: str
    client_name: Optional[str]
    confidence_score: int
    reasoning: str

class AnalysisResponse(BaseModel):
    documents: List[LogicalDocument]