import pytest
import zipfile
from pathlib import Path
from extract_and_flatten import DocumentIngestionProcessor, ZipExtractionError

@pytest.fixture
def sample_zip(tmp_path: Path) -> Path:
    """Crea un ZIP con estructura compleja para validación de pruebas."""
    zip_path = tmp_path / "test_input.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        # Archivos válidos en subcarpetas
        zf.writestr("folder_a/document_1.pdf", b"content_1")
        zf.writestr("folder_b/sub/document_2.pdf", b"content_2")
        # Archivos de sistema a ignorar
        zf.writestr("folder_a/Thumbs.db", b"trash")
        zf.writestr(".DS_Store", b"trash")
        # Colisión potencial
        zf.writestr("folder_c/document_1.pdf", b"content_overlap")
    return zip_path

def test_processor_flattens_and_filters_correctly(sample_zip: Path, tmp_path: Path) -> None:
    """
    Valida que el procesador elimine carpetas, ignore basura y 
    mantenga archivos válidos.
    """
    target_dir = tmp_path / "raws"
    processor = DocumentIngestionProcessor(sample_zip, target_dir)
    
    processor.process()
    
    # Comprobación de existencia (aplanado)
    assert (target_dir / "document_1.pdf").exists()
    assert (target_dir / "document_2.pdf").exists()
    
    # Comprobación de resolución de colisiones
    assert (target_dir / "document_1_1.pdf").exists()
    
    # Comprobación de filtrado
    assert not (target_dir / "Thumbs.db").exists()
    assert not (target_dir / ".DS_Store").exists()
    
    # Cantidad total de archivos válidos (2 originales + 1 colisión)
    assert len(list(target_dir.iterdir())) == 3

def test_processor_raises_error_on_corrupt_zip(tmp_path: Path) -> None:
    """Verifica el manejo de excepciones ante archivos ZIP inválidos."""
    corrupt_file = tmp_path / "bad.zip"
    corrupt_file.write_text("no es un zip")
    
    target_dir = tmp_path / "output"
    processor = DocumentIngestionProcessor(corrupt_file, target_dir)
    
    with pytest.raises(ZipExtractionError, match="archivo ZIP está corrupto"):
        processor.process()

def test_unique_path_generation_logic(tmp_path: Path) -> None:
    """Valida la lógica de nombrado único de forma aislada."""
    target_dir = tmp_path / "test_unique"
    target_dir.mkdir()
    
    file_path = target_dir / "test.txt"
    file_path.write_text("original")
    
    processor = DocumentIngestionProcessor(Path("dummy.zip"), target_dir)
    new_path = processor._generate_unique_path(file_path)
    
    assert new_path.name == "test_1.txt"
    assert not new_path.exists()