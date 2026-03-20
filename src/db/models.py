"""Modelos de dominio para persistencia relacional.

Mapea la estructura de metadatos documentales hacia el esquema DDL de PostgreSQL.
Garantiza la unicidad y el versionamiento condicionado mediante restricciones compuestas.
"""

from datetime import datetime, timezone
import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RegistroArtefacto(Base):
    """Entidad que representa un documento físico procesado y consolidado."""
    
    __tablename__ = 'registro_artefactos'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    folio = Column(String(100), nullable=False, index=True)
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
    
    fecha_procesamiento = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('folio', 'version', 'archivo_original', name='uix_folio_version_origen'),
    )