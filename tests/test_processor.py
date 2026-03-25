"""Pruebas unitarias para el orquestador del pipeline documental.

Aísla la lógica de concurrencia y valida el patrón Map-Reduce y
el procesamiento por Generadores (Lotes Transaccionales).
"""

import pytest
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

from src.core.processor import PipelineProcessor, IDocumentAnalyzer
from src.models import AnalysisResponse, LogicalDocument, PageInstruction

@pytest.fixture
def mock_repo() -> MagicMock:
    repo = MagicMock()
    repo.resolve_versioning.return_value = (1, "NUEVO")
    return repo

@pytest.fixture
def mock_analyzer() -> MagicMock:
    return MagicMock(spec=IDocumentAnalyzer)

@pytest.fixture
def mock_settings(tmp_path: Path) -> Generator[MagicMock, None, None]:
    """Crea una estructura de directorios temporal para evitar afectar tu disco real."""
    with patch("src.core.processor.settings") as mock_set:
        mock_set.raw_dir = tmp_path / "raw"
        mock_set.explosion_dir = tmp_path / "explosion"
        mock_set.final_dir = tmp_path / "final"
        mock_set.processed_dir = tmp_path / "processed"
        mock_set.error_ilegible = "ERROR_DOCUMENTO_ILEGIBLE"
        mock_set.max_threads = 2
        mock_set.vision_batch_size = 2  
        mock_set.pdf_chunk_max_pages = 100
        mock_set.physical_chunk_size = 50
        
        mock_set.raw_dir.mkdir()
        mock_set.explosion_dir.mkdir()
        mock_set.final_dir.mkdir()
        mock_set.processed_dir.mkdir()
        
        yield mock_set

@pytest.fixture
def processor(mock_settings: MagicMock, mock_repo: MagicMock, mock_analyzer: MagicMock) -> PipelineProcessor:
    return PipelineProcessor(analyzer=mock_analyzer, db_repo=mock_repo)

def test_pipeline_empty_directory(processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Verifica que el orquestador aborte limpiamente si no hay carga de trabajo."""
    # Extraemos los resultados del Generador (Yield)
    results = [item for chunk in processor.run() for item in chunk]
    assert len(results) == 0

@patch("src.core.processor.fitz.open")
@patch("src.core.processor.PDFToolbox")
def test_pipeline_processes_and_replicates_pdf(mock_toolbox: MagicMock, mock_fitz: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Valida el flujo de PDFs y la inyección de versiones en bases de datos."""
    dummy_pdf = mock_settings.raw_dir / "factura_multiple.pdf"
    dummy_pdf.touch()

    # Simulamos que el PDF tiene 1 página para el algoritmo de empaquetado (Greedy Bin Packing)
    mock_fitz.return_value.__enter__.return_value.__len__.return_value = 1

    exploded_page = mock_settings.explosion_dir / "factura_multiple_P001.pdf"
    mock_toolbox.explode_pdf.return_value = [exploded_page]
    mock_toolbox.merge_by_folio.return_value = Path("A12052.pdf")

    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["A12052"],
            pages=[PageInstruction(file_name=exploded_page.name, rotation_degrees=0)],
            document_type="Factura",
            client_name="Cliente A",
            confidence_score=95,
            reasoning="Validación satisfactoria."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = [item for chunk in processor.run() for item in chunk]

    assert len(results) == 1
    assert results[0]["Folio"] == "A12052"
    assert results[0]["Categoría"] == "Factura"
    assert results[0]["Versión"] == 1
    
    mock_toolbox.merge_by_folio.assert_called_once()
    processor.db_repo.resolve_versioning.assert_called_once_with("A12052", "Factura", "factura_multiple.pdf", 95)

@patch("src.core.processor.fitz.open")
@patch("src.core.processor.PDFToolbox")
def test_pipeline_orphan_catcher_logic(mock_toolbox: MagicMock, mock_fitz: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Asegura que las páginas excluidas lógicamente sean rescatadas a revisión manual."""
    dummy_pdf = mock_settings.raw_dir / "documento_huerfano.pdf"
    dummy_pdf.touch()

    mock_fitz.return_value.__enter__.return_value.__len__.return_value = 2

    page_1 = mock_settings.explosion_dir / "doc_P001.pdf"
    page_2 = mock_settings.explosion_dir / "doc_P002.pdf"
    mock_toolbox.explode_pdf.return_value = [page_1, page_2]
    mock_toolbox.merge_by_folio.return_value = Path("rescue.pdf")

    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_VALIDO"],
            pages=[PageInstruction(file_name=page_1.name, rotation_degrees=0)],
            document_type="Factura",
            client_name="Cliente B",
            confidence_score=90,
            reasoning="Se omitió voluntariamente la hoja."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = [item for chunk in processor.run() for item in chunk]

    assert len(results) == 2
    doc_valido = next(r for r in results if r["Folio"] == "FOLIO_VALIDO")
    assert doc_valido["Páginas Final"] == 1

@patch("src.core.processor.fitz.open")
@patch("src.core.processor.PDFToolbox")
def test_pipeline_discard_action_skips_processing(mock_toolbox: MagicMock, mock_fitz: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Garantiza que no se escriba en disco cuando la BD ordena DESCARTAR por colisión."""
    dummy_pdf = mock_settings.raw_dir / "archivo_duplicado.pdf"
    dummy_pdf.touch()

    mock_fitz.return_value.__enter__.return_value.__len__.return_value = 1
    exploded_page = mock_settings.explosion_dir / "archivo_duplicado_P001.pdf"
    mock_toolbox.explode_pdf.return_value = [exploded_page]
    
    processor.db_repo.resolve_versioning.return_value = (1, "DESCARTAR")

    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_DUPLICADO"],
            pages=[PageInstruction(file_name=exploded_page.name, rotation_degrees=0)],
            document_type="Factura",
            client_name="Cliente C",
            confidence_score=40,
            reasoning="Colisión."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = [item for chunk in processor.run() for item in chunk]

    assert len(results) == 0
    mock_toolbox.merge_by_folio.assert_not_called()

@patch("src.core.processor.PDFToolbox")
def test_pipeline_loose_images_batching_solves_split_batch(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Verifica que imágenes sueltas enviadas en distintas ráfagas a Gemini se agrupen correctamente."""
    for i in range(3):
        (mock_settings.raw_dir / f"img_{i}.jpg").touch()

    mock_toolbox.wrap_image_to_pdf.side_effect = lambda path, out_dir: out_dir / f"{path.stem}.pdf"
    mock_toolbox.merge_by_folio.return_value = Path("FOLIO_IMG.pdf")

    mock_analysis_lote1 = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_IMG"],
            pages=[PageInstruction(file_name="img_0.pdf", rotation_degrees=0), 
                   PageInstruction(file_name="img_1.pdf", rotation_degrees=0)],
            document_type="Ticket", client_name="N/A", confidence_score=95, reasoning="Parte 1."
        )
    ])
    
    mock_analysis_lote2 = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_IMG"], 
            pages=[PageInstruction(file_name="img_2.pdf", rotation_degrees=0)],
            document_type="Ticket", client_name="N/A", confidence_score=95, reasoning="Parte 2."
        )
    ])
    
    processor.analyzer.analyze_batch.side_effect = [mock_analysis_lote1, mock_analysis_lote2]

    results = [item for chunk in processor.run() for item in chunk]

    assert processor.analyzer.analyze_batch.call_count == 2
    assert len(results) == 1
    assert results[0]["Páginas Final"] == 3