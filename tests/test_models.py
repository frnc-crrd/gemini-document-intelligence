"""Pruebas unitarias para validación estricta de esquemas de datos."""

import pytest
from pydantic import ValidationError
from src.models import LogicalDocExtraction, PageInstruction


def test_logical_doc_extraction_valid() -> None:
    """Verifica que el modelo acepte tipos correctos."""
    data = {
        "folios": ["A-100", "A-101"],
        "ordered_file_names": ["page_001.pdf", "page_002.pdf"],
        "document_type": "Factura",
        "client_name": "Empresa X",
        "confidence_score": 95,
        "reasoning": "Validación por sellos concordantes."
    }
    obj = LogicalDocExtraction(**data)
    assert obj.document_type == "Factura"
    assert obj.confidence_score == 95


def test_logical_doc_extraction_invalid_confidence() -> None:
    """Verifica el cumplimiento de restricciones de límites en score."""
    data = {
        "folios": ["A-100"],
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
        PageInstruction(file_name="test.pdf", rotation_degrees="noventa") # No es un entero