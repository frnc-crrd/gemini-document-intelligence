"""Pruebas unitarias para el orquestador del pipeline documental.

Aísla la lógica de concurrencia (ThreadPoolExecutor) y las operaciones de
entrada/salida (I/O) utilizando Mocks. Implementa pruebas para la Inyección 
de Dependencias sin requerir parches globales (Monkeypatching) sobre clases concretas.
"""

import pytest
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

from src.core.processor import PipelineProcessor, IDocumentAnalyzer
from src.models import AnalysisResponse, LogicalDocument, PageInstruction


@pytest.fixture
def mock_repo() -> MagicMock:
    """Inyecta un repositorio simulado para el control de versiones transaccional."""
    repo = MagicMock()
    repo.resolve_versioning.return_value = (1, "NUEVO")
    return repo


@pytest.fixture
def mock_analyzer() -> MagicMock:
    """Provee un mock formal basado en el contrato de IDocumentAnalyzer."""
    return MagicMock(spec=IDocumentAnalyzer)


@pytest.fixture
def mock_settings(tmp_path: Path) -> Generator[MagicMock, None, None]:
    """Configura un entorno de directorios efímero para aislar las operaciones de disco."""
    with patch("src.core.processor.settings") as mock_set:
        mock_set.raw_dir = tmp_path / "raw"
        mock_set.explosion_dir = tmp_path / "explosion"
        mock_set.final_dir = tmp_path / "final"
        mock_set.error_ilegible = "ERROR_DOCUMENTO_ILEGIBLE"
        mock_set.max_threads = 2
        mock_set.vision_batch_size = 2  
        
        mock_set.raw_dir.mkdir()
        mock_set.explosion_dir.mkdir()
        mock_set.final_dir.mkdir()
        
        yield mock_set


@pytest.fixture
def processor(mock_settings: MagicMock, mock_repo: MagicMock, mock_analyzer: MagicMock) -> PipelineProcessor:
    """Instancia un procesador inyectando explícitamente las dependencias de prueba."""
    return PipelineProcessor(analyzer=mock_analyzer, db_repo=mock_repo)


def test_pipeline_empty_directory(processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Verifica que el orquestador aborte limpiamente si no hay carga de trabajo en el directorio raw."""
    results = processor.run()
    assert len(results) == 0


@patch("src.core.processor.PDFToolbox")
def test_pipeline_processes_and_replicates_pdf(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Valida el flujo de procesamiento primario, propagación de categoría y la inyección correcta de la versión."""
    dummy_pdf = mock_settings.raw_dir / "factura_multiple.pdf"
    dummy_pdf.touch()

    exploded_page = mock_settings.explosion_dir / "factura_multiple_P001.pdf"
    mock_toolbox.explode_pdf.return_value = [exploded_page]
    mock_toolbox.merge_by_folio.return_value = Path("FOLIO_1.pdf")

    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_1"],
            pages=[PageInstruction(file_name=exploded_page.name, rotation_degrees=0)],
            document_type="Remisión",
            client_name="Cliente A",
            confidence_score=95,
            reasoning="Validación satisfactoria."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = processor.run()

    assert len(results) == 1
    assert results[0]["Folio"] == "FOLIO_1"
    assert results[0]["Categoría"] == "Remisión"
    assert results[0]["Versión"] == 1
    
    mock_toolbox.merge_by_folio.assert_called_once()
    processor.db_repo.resolve_versioning.assert_called_once_with("FOLIO_1", "Remisión", "factura_multiple.pdf", 95)


@patch("src.core.processor.PDFToolbox")
def test_pipeline_orphan_catcher_logic(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Asegura que las páginas excluidas lógicamente por la IA sean rescatadas forzosamente por conservación de masa."""
    dummy_pdf = mock_settings.raw_dir / "documento_huerfano.pdf"
    dummy_pdf.touch()

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
            reasoning="Se omitió voluntariamente la hoja de sellos."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = processor.run()

    assert len(results) == 2
    
    doc_valido = next(r for r in results if r["Folio"] == "FOLIO_VALIDO")
    assert doc_valido["Páginas Final"] == 1
    
    doc_huerfano = next(r for r in results if r["Categoría"] == "Página Huérfana")
    assert doc_huerfano["Status"] == "REVISIÓN MANUAL"
    assert doc_huerfano["Páginas Final"] == 1


@patch("src.core.processor.PDFToolbox")
def test_pipeline_discard_action_skips_processing(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Garantiza que el orquestador aborte la escritura en disco cuando el Repositorio ordena DESCARTAR el registro."""
    dummy_pdf = mock_settings.raw_dir / "archivo_duplicado.pdf"
    dummy_pdf.touch()

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
            reasoning="Extracción deficiente y colisión."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = processor.run()

    assert len(results) == 0
    mock_toolbox.merge_by_folio.assert_not_called()


@patch("src.core.processor.PDFToolbox")
def test_pipeline_loose_images_batching(mock_toolbox: MagicMock, processor: PipelineProcessor, mock_settings: MagicMock) -> None:
    """Verifica que el orquestador divida un conjunto grande de imágenes en lotes secuenciales."""
    for i in range(3):
        (mock_settings.raw_dir / f"img_{i}.jpg").touch()

    mock_toolbox.wrap_image_to_pdf.side_effect = lambda path, out_dir: out_dir / f"{path.stem}.pdf"
    mock_toolbox.merge_by_folio.return_value = Path("FOLIO_IMG.pdf")

    mock_analysis = AnalysisResponse(documents=[
        LogicalDocument(
            folios=["FOLIO_IMG"],
            pages=[PageInstruction(file_name="img_0.pdf", rotation_degrees=0)],
            document_type="Ticket",
            client_name="N/A",
            confidence_score=95,
            reasoning="Validado."
        )
    ])
    processor.analyzer.analyze_batch.return_value = mock_analysis

    results = processor.run()

    assert processor.analyzer.analyze_batch.call_count == 2
    assert len(results) > 0