"""Pruebas unitarias para validación estricta de esquemas de datos."""

import pytest
from pydantic import ValidationError
from src.models import LogicalDocExtraction, PageInstruction, PageOrientation, PageRole


def test_logical_doc_extraction_valid() -> None:
    """Verifica que el modelo acepte tipos correctos y el esquema de secuenciación."""
    data = {
        "folios": ["A-100"],
        "page_roles": [
            {"file_name": "page_001.pdf", "role": "INICIO", "evidence": "Membrete y folio."},
            {"file_name": "page_002.pdf", "role": "CIERRE", "evidence": "Sello QR en footer."}
        ],
        "ordered_file_names": ["page_001.pdf", "page_002.pdf"],
        "document_type": "Factura",
        "client_name": "Empresa X",
        "confidence_score": 95,
        "reasoning": "Validación por sellos concordantes."
    }
    obj = LogicalDocExtraction(**data)
    assert obj.document_type == "Factura"
    assert obj.confidence_score == 95
    assert len(obj.page_roles) == 2


def test_page_role_invalid_literal() -> None:
    """Verifica que la clasificación topológica rechace roles no definidos en la heurística."""
    with pytest.raises(ValidationError):
        PageRole(file_name="page_001.pdf", role="PORTADA", evidence="Es la portada")


def test_logical_doc_extraction_invalid_confidence() -> None:
    """Verifica el cumplimiento de restricciones de límites en score."""
    data = {
        "folios": ["A-100"],
        "page_roles": [{"file_name": "page_001.pdf", "role": "UNICA", "evidence": "Todo en una hoja."}],
        "ordered_file_names": ["page_001.pdf"],
        "document_type": "Factura",
        "confidence_score": 105,  # Viola le=100
        "reasoning": "Test."
    }
    with pytest.raises(ValidationError):
        LogicalDocExtraction(**data)


def test_page_instruction_types() -> None:
    """Asegura el rechazo de tipado incorrecto en las instrucciones físicas."""
    with pytest.raises(ValidationError):
        PageInstruction(file_name="test.pdf", rotation_degrees="noventa")


def test_page_orientation_strict_validation() -> None:
    """Verifica que el agente espacial limite la rotación por el validador estricto."""
    valid_data = {
        "file_name": "scan_001.pdf",
        "rotation_degrees": 90,
        "reasoning": "Texto en dirección ascendente."
    }
    obj = PageOrientation(**valid_data)
    assert obj.rotation_degrees == 90
    
    invalid_data = {
        "file_name": "scan_001.pdf",
        "rotation_degrees": 45,  # Ángulo no permitido
        "reasoning": "Inclinación parcial."
    }
    with pytest.raises(ValidationError):
        PageOrientation(**invalid_data)