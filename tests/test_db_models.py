"""Pruebas unitarias para el modelo de dominio relacional.

Valida el contrato DDL mediante introspección de SQLAlchemy, garantizando
que las restricciones de nulidad, valores por defecto, indexación y
reglas de versionamiento estén correctamente configuradas.
"""

from sqlalchemy import UniqueConstraint
from src.db.models import RegistroArtefacto


def test_registro_artefacto_schema_definition() -> None:
    """Verifica la configuración de las columnas críticas, índices y llave de versionamiento."""
    columns = RegistroArtefacto.__table__.columns

    # Validación de llaves e índices primarios
    assert "id" in columns
    assert columns["id"].primary_key is True
    
    # Validación de versionamiento e identidad categórica
    assert "version" in columns
    assert columns["version"].nullable is False
    assert columns["version"].default.arg == 1
    
    # Validación de restricciones de nulidad (NOT NULL)
    assert columns["folio"].nullable is False
    assert columns["categoria"].nullable is False
    assert columns["ruta_servidor"].nullable is False
    assert columns["status"].nullable is False
    
    # Validación de unicidad de rutas
    assert columns["ruta_servidor"].unique is True

    # Validación de valores por defecto (Default constraints)
    assert columns["categoria"].default.arg == "No identificado"
    assert columns["divisa"].default.arg == "NO_DETECTADA"
    assert columns["cliente"].default.arg == "NO DETECTADO"
    assert columns["estado_conciliacion"].default.arg == "PENDIENTE"

    # Validación de indexación (B-Tree) requerida para cruce con Firebird
    assert columns["folio"].index is True
    assert columns["categoria"].index is True
    assert columns["cliente"].index is True
    assert columns["status"].index is True
    assert columns["estado_conciliacion"].index is True

    # Validación de restricción única compuesta (Deduplicación)
    constraints = RegistroArtefacto.__table__.constraints
    
    # Buscamos explícitamente la restricción compuesta por nombre para evitar
    # colisiones con las restricciones únicas implícitas de SQLAlchemy (ej. ruta_servidor)
    target_constraint = next(
        (c for c in constraints if isinstance(c, UniqueConstraint) and c.name == 'uix_folio_categoria_version_origen'), 
        None
    )
    
    assert target_constraint is not None, "La restricción 'uix_folio_categoria_version_origen' no fue definida."
    constraint_cols = [col.name for col in target_constraint.columns]
    assert "folio" in constraint_cols
    assert "categoria" in constraint_cols
    assert "version" in constraint_cols
    assert "archivo_original" in constraint_cols