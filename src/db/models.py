"""Modelos de dominio para persistencia relacional.

Mapea la estructura de metadatos documentales hacia el esquema DDL de PostgreSQL.
Garantiza la unicidad y el versionamiento condicionado mediante restricciones compuestas.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

# Ajuste de Zona Horaria a Gómez Palacio, Durango
tz_local = ZoneInfo("America/Monterrey")
Base = declarative_base()


class RegistroArtefacto(Base):
    """Entidad que representa un documento físico procesado y consolidado."""
    
    __tablename__ = 'registro_artefactos'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    folio = Column(String(100), nullable=False, index=True)
    categoria = Column(String(100), nullable=False, index=True, default='No identificado')
    version = Column(Integer, default=1, nullable=False)
    divisa = Column(String(20), default='NO_DETECTADA')
    cliente = Column(String(255), default='NO DETECTADO', index=True)
    archivo_original = Column(String(255), nullable=False)
    paginas_consolidado = Column(Integer, nullable=False)
    status = Column(String(50), nullable=False, index=True)
    confianza_promedio = Column(Integer, nullable=False)
    ruta_servidor = Column(String(500), nullable=False, unique=True)
    justificacion = Column(Text, nullable=True)
    
    estado_conciliacion = Column(String(50), default='PENDIENTE', index=True)
    id_docto_firebird = Column(String(100), nullable=True)
    
    fecha_procesamiento = Column(DateTime(timezone=True), default=lambda: datetime.now(tz_local))

    __table_args__ = (
        UniqueConstraint('folio', 'categoria', 'version', 'archivo_original', name='uix_folio_categoria_version_origen'),
    )