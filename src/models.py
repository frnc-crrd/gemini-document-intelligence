"""Modelos de datos y esquemas de validación para el sistema de integridad documental.

Implementa estructuras fuertemente tipadas utilizando Pydantic V2 para gobernar
las entradas y salidas del pipeline Multi-Agente (GenAI). Estas clases definen
el contrato de datos estricto que la inteligencia artificial debe respetar.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


# ==========================================
# ESQUEMAS: AGENTE 1 (ESPACIAL)
# ==========================================

class PageOrientation(BaseModel):
    """Modelo de clasificación espacial para una página individual."""
    
    model_config = ConfigDict(strict=True)
    
    file_name: str = Field(
        ..., 
        description="Identificador exacto del nombre de archivo analizado."
    )
    orientacion: str = Field(
        ..., 
        description="Dirección física de lectura. Valores permitidos: 'NORMAL', 'ACOSTADO_IZQUIERDA', 'ACOSTADO_DERECHA', 'INVERTIDO'."
    )


class OrientationResponse(BaseModel):
    """Estructura de respuesta aglomerada del Agente Espacial."""
    
    model_config = ConfigDict(strict=True)
    
    orientations: List[PageOrientation] = Field(
        ..., 
        description="Lista de orientaciones detectadas por cada archivo procesado."
    )


# ==========================================
# ESQUEMAS: AGENTE 2 (AUDITOR)
# ==========================================

class LogicalDocExtraction(BaseModel):
    """Modelo de extracción semántica que agrupa páginas en una entidad lógica."""
    
    model_config = ConfigDict(strict=True)
    
    folios: List[str] = Field(
        ..., 
        description="Arreglo de folios detectados. Incluye todos los impresos si es un reporte o diario."
    )
    ordered_file_names: List[str] = Field(
        ..., 
        description="Nombres de archivo en estricto orden cronológico de lectura documental (sellos SAT al final)."
    )
    document_type: str = Field(
        ..., 
        description="Categoría semántica del documento (e.g., Factura, Remisión)."
    )
    client_name: Optional[str] = Field(
        None, 
        description="Razón social o nombre comercial del cliente identificado, si aplica."
    )
    confidence_score: int = Field(
        ..., 
        ge=0, 
        le=100, 
        description="Métrica de certeza sobre la exactitud de la extracción y emparejamiento (0 a 100)."
    )
    reasoning: str = Field(
        ..., 
        description="Justificación técnica y lógica de las decisiones tomadas por el agente durante la extracción."
    )


class ExtractionResponse(BaseModel):
    """Estructura de respuesta aglomerada del Agente Auditor."""
    
    model_config = ConfigDict(strict=True)
    
    documents: List[LogicalDocExtraction] = Field(
        ..., 
        description="Lista de documentos lógicos reconstruidos a partir de los fragmentos físicos."
    )


# ==========================================
# ESQUEMA FINAL (PARA EL ORQUESTADOR)
# ==========================================

class PageInstruction(BaseModel):
    """Instrucción de ensamblaje físico para una página específica."""
    
    model_config = ConfigDict(strict=True)
    
    file_name: str = Field(..., description="Nombre del archivo físico en disco.")
    rotation_degrees: int = Field(..., description="Grados de rotación en sentido horario a aplicar (0, 90, 180, 270).")


class LogicalDocument(BaseModel):
    """Representación integral de un documento auditado y validado."""
    
    model_config = ConfigDict(strict=True)
    
    folios: List[str] = Field(..., description="Identificadores del documento.")
    pages: List[PageInstruction] = Field(..., description="Secuencia de páginas con instrucciones de corrección espacial.")
    document_type: str = Field(..., description="Tipo de documento determinado por la IA.")
    client_name: Optional[str] = Field(None, description="Cliente asociado.")
    confidence_score: int = Field(..., description="Nivel de confianza general de la extracción.")
    reasoning: str = Field(..., description="Traza de auditoría generada por el agente.")


class AnalysisResponse(BaseModel):
    """Respuesta final encapsulada emitida por el motor de análisis completo."""
    
    model_config = ConfigDict(strict=True)
    
    documents: List[LogicalDocument] = Field(..., description="Documentos finales listos para ser reconstruidos físicamente.")