"""Pruebas unitarias para el orquestador del pipeline documental.

Aísla la lógica de concurrencia (ThreadPoolExecutor) y las operaciones de
entrada/salida (I/O) utilizando Mocks. Valida explícitamente el algoritmo
de conservación de masa (Recogedor de Huérfanos).
"""

import pytest
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

from src.core.processor import PipelineProcessor
from src.models import AnalysisResponse, LogicalDocument, PageInstruction


@pytest.fixture
def mock_settings(tmp_path: Path) -> Generator[MagicMock, None, None]:
    """Configura un entorno de directorios efímero para aislar las pruebas."""
    with patch("src.core.processor.settings") as mock_set:
        mock_set.raw_dir = tmp_path / "raw"
        mock_set.explosion_dir = tmp_path / "explosion"
        mock_set.final_dir = tmp_path / "final"
        mock_set.error_ilegible = "ERROR_DOCUMENTO_ILEGIBLE"
        
        mock_set.raw_dir.mkdir()
        mock_set.explosion_dir.mkdir()
        mock_set.final_dir.mkdir()
        
        yield mock_set


@pytest.fixture
def processor(mock_settings: MagicMock) -> PipelineProcessor:
    """Inyecta un procesador con el analizador de IA interceptado."""
    with patch("src.core.processor.DocumentAnalyzer"):
        proc = PipelineProcessor()
        # Forzamos concurrencia secuencial en pruebas para evitar condiciones de carrera
        proc.max_workers = 1 
        return proc


def test_pipeline_empty_directory(processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Verifica que el orquestador aborte limpiamente si no hay carga de trabajo."""
    # El directorio raw_dir está vacío por defecto en el fixture
    results = processor.run()
    assert len(results) == 0


@patch("src.core.processor.PDFToolbox")
def test_pipeline_processes_pdf_successfully(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Valida el flujo feliz de procesamiento de un archivo PDF válido."""
    # Preparar el entorno físico simulado
    dummy_pdf = mock_settings.raw_dir / "factura_test.pdf"
    dummy_pdf.touch()

    # Simular la explosión física del documento
    exploded_page = mock_settings.explosion_dir / "factura_test_P001.pdf"
    mock_toolbox.explode_pdf.return_value = [exploded_page]
    mock_toolbox.merge_by_folio.return_value = Path("final_123.pdf")

    # Simular la respuesta semántica de la IA
    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_123"],
            pages=[PageInstruction(file_name=exploded_page.name, rotation_degrees=0)],
            document_type="Factura",
            client_name="Cliente A",
            confidence_score=95,
            reasoning="Validación exitosa"
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    # Ejecución
    results = processor.run()

    # Afirmaciones (Assertions)
    assert len(results) == 1
    assert results[0]["Folio"] == "FOLIO_123"
    assert results[0]["Categoría"] == "Factura"
    assert results[0]["Status"] == "OK"
    assert results[0]["Páginas Original"] == 1
    assert results[0]["Páginas Final"] == 1
    
    mock_toolbox.explode_pdf.assert_called_once_with(dummy_pdf, mock_settings.explosion_dir)
    mock_toolbox.merge_by_folio.assert_called_once()


@patch("src.core.processor.PDFToolbox")
def test_pipeline_orphan_catcher_logic(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Asegura que las páginas ignoradas por la IA sean rescatadas por conservación de masa."""
    dummy_pdf = mock_settings.raw_dir / "documento_largo.pdf"
    dummy_pdf.touch()

    # Simulamos que el PDF se dividió en 2 páginas
    page_1 = mock_settings.explosion_dir / "doc_P001.pdf"
    page_2 = mock_settings.explosion_dir / "doc_P002.pdf"
    mock_toolbox.explode_pdf.return_value = [page_1, page_2]
    mock_toolbox.merge_by_folio.return_value = Path("rescue.pdf")

    # Simulamos que la IA *solo* utilizó la página 1 y omitió la 2
    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_VALIDO"],
            pages=[PageInstruction(file_name=page_1.name, rotation_degrees=0)],
            document_type="Remisión",
            client_name="Cliente B",
            confidence_score=90,
            reasoning="Se omitió la hoja de sellos."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    # Ejecución
    results = processor.run()

    # El resultado debe contener 2 registros: 1 del documento procesado, 1 del huérfano rescatado
    assert len(results) == 2
    
    # Validamos el documento exitoso
    doc_valido = next(r for r in results if r["Folio"] == "FOLIO_VALIDO")
    assert doc_valido["Páginas Final"] == 1
    
    # Validamos la activación del algoritmo de rescate
    doc_huerfano = next(r for r in results if r["Categoría"] == "Página Huérfana")
    assert doc_huerfano["Status"] == "REVISIÓN MANUAL"
    assert doc_huerfano["Páginas Final"] == 1
    assert doc_huerfano["Folio"] == mock_settings.error_ilegible
    assert "rescata" in doc_huerfano["Justificación"].lower() or "excluido" in doc_huerfano["Justificación"].lower()