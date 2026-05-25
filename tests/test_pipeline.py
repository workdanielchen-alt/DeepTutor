"""Tests for the document classification pipeline (RagDocumentPipeline)."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from tutor_platform.rag.config import PipelineConfig
from tutor_platform.rag.context import ProcessingContext
from tutor_platform.rag.pipeline import RagDocumentPipeline

# ===================================================================
# Fixtures: real (tiny) PDFs created via PyMuPDF
# ===================================================================

pytestmark = pytest.mark.skipif(
    not pytest.importorskip("fitz", reason="PyMuPDF not installed"),
    reason="PyMuPDF required to build test PDF fixtures",
)


@pytest.fixture
def text_pdf(tmp_path: Path) -> Path:
    """A PDF with a real text layer (each page > 50 chars)."""
    import fitz
    path = tmp_path / "text.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), "Page one has a long meaningful text layer for testing purposes to exceed the threshold.")
    page2 = doc.new_page()
    page2.insert_text((50, 50), "Second page also has a sufficiently long text content to pass the text layer check.")
    doc.save(str(path))
    doc.close()
    return path


@pytest.fixture
def scanned_pdf(tmp_path: Path) -> Path:
    """A PDF with no text layer (simulates a scanned document)."""
    import fitz
    path = tmp_path / "scanned.pdf"
    doc = fitz.open()
    doc.new_page()  # blank page → no text
    doc.new_page()  # another blank page
    doc.save(str(path))
    doc.close()
    return path


@pytest.fixture
def mixed_pdf(tmp_path: Path) -> Path:
    """A PDF where some pages have text and others don't."""
    import fitz
    path = tmp_path / "mixed.pdf"
    doc = fitz.open()
    # Page 1: has text (> 50 chars)
    p1 = doc.new_page()
    p1.insert_text((50, 50), "This page has a long text content that exceeds the fifty character threshold comfortably.")
    # Page 2: no text (simulating scanned)
    doc.new_page()
    # Page 3: has text (> 50 chars)
    p3 = doc.new_page()
    p3.insert_text((50, 50), "Third page also has a long text content that exceeds the fifty character threshold.")
    # Page 4: no text
    doc.new_page()
    doc.save(str(path))
    doc.close()
    return path


@pytest.fixture
def existing_sidecar_pdf(tmp_path: Path) -> Path:
    """A scanned PDF that already has a ``.ocr.txt`` sidecar."""
    import fitz
    path = tmp_path / "existing.pdf"
    doc = fitz.open()
    doc.new_page()
    doc.save(str(path))
    doc.close()
    # Pre-create sidecar
    sidecar = path.with_name(path.stem + ".ocr.txt")
    sidecar.write_text("Existing OCR content.", encoding="utf-8")
    return path


@pytest.fixture
def fake_pdf_text(tmp_path: Path) -> Path:
    """A ``.pdf`` file that is actually UTF-8 text (gateway OCR output)."""
    path = tmp_path / "document.pdf"
    path.write_text(
        "七年级\n上册\n数学\n全国优秀教材二等奖\n\n"
        "This is a simulated OCR output from the WeChat gateway. "
        "It contains enough meaningful text to exceed the threshold.",
        encoding="utf-8",
    )
    return path


# ===================================================================
# Helpers
# ===================================================================

async def run_pipeline(file_paths: list[str], **config_kw) -> list[str]:
    """Run the pipeline with an optional ad-hoc config override."""
    if config_kw:
        cfg = PipelineConfig(**config_kw)
        RagDocumentPipeline.set_config(cfg)
    try:
        return await RagDocumentPipeline.process(file_paths)
    finally:
        RagDocumentPipeline._config = None  # reset for subsequent tests


# ===================================================================
# Tests
# ===================================================================


class TestProcess:
    """RagDocumentPipeline.process() — high-level behaviour."""

    @pytest.mark.asyncio
    async def test_empty_list(self):
        assert await run_pipeline([]) == []

    @pytest.mark.asyncio
    async def test_non_pdf_files_unchanged(self, tmp_path):
        txt = tmp_path / "hello.txt"
        txt.write_text("hello", encoding="utf-8")
        result = await run_pipeline([str(txt)])
        assert result == [str(txt)]

    @pytest.mark.asyncio
    async def test_text_pdf_skips_ocr(self, text_pdf, mock_llm_client):
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            result = await run_pipeline([str(text_pdf)])
        # No sidecar created — only the original path
        assert result == [str(text_pdf)]
        mock_llm_client.complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_scanned_pdf_creates_sidecar(self, scanned_pdf, mock_llm_client):
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            result = await run_pipeline([str(scanned_pdf)])
        assert len(result) == 2
        assert result[0] == str(scanned_pdf)
        sidecar_path = result[1]
        assert sidecar_path.endswith(".ocr.txt")
        assert os.path.isfile(sidecar_path)
        content = Path(sidecar_path).read_text(encoding="utf-8")
        assert "Mocked OCR text content." in content
        # Each page should have been OCR'd
        assert mock_llm_client.complete.await_count == 2

    @pytest.mark.asyncio
    async def test_mixed_pdf_partial_ocr(self, mixed_pdf, mock_llm_client):
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            result = await run_pipeline([str(mixed_pdf)])
        # mixed.pdf has 4 pages: text, blank, text, blank → 2 pages should be OCR'd
        assert len(result) == 2
        assert result[0] == str(mixed_pdf)
        assert result[1].endswith(".ocr.txt")
        assert mock_llm_client.complete.await_count == 2  # 2 pages without text
        content = Path(result[1]).read_text(encoding="utf-8")
        assert "--- Page 1 ---" in content
        assert "--- Page 3 ---" in content

    @pytest.mark.asyncio
    async def test_existing_sidecar_dedup(self, existing_sidecar_pdf, mock_llm_client):
        """Re-running should not create duplicate paths."""
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            # First run — creates sidecar
            result1 = await run_pipeline([str(existing_sidecar_pdf)])
            assert len(result1) == 2
            assert result1[1].endswith(".ocr.txt")
            # Second run — pipeline re-OCRs but dedup removes the duplicate
            result2 = await run_pipeline(result1)
            assert len(result2) == 2
            assert result2[0] == str(existing_sidecar_pdf)
            assert result2[1].endswith(".ocr.txt")

    @pytest.mark.asyncio
    async def test_config_disabled_skips_ocr(self, scanned_pdf, mock_llm_client):
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            result = await run_pipeline([str(scanned_pdf)], ocr_enabled=False)
        assert result == [str(scanned_pdf)]
        mock_llm_client.complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_llm_timeout_retry_logs_error(self, scanned_pdf, mock_llm_client):
        """LLM timeout should retry then degrade gracefully with error recorded."""
        import asyncio

        mock_llm_client.complete = AsyncMock(side_effect=asyncio.TimeoutError("timeout"))
        with patch("tutor_platform.rag.pipeline.get_llm_client", return_value=mock_llm_client):
            result = await run_pipeline(
                [str(scanned_pdf)],
                ocr_page_timeout=0.1,
                ocr_max_retries=1,
                ocr_retry_delay=0.01,
            )
        # Even with failure, the sidecar is created (with empty text for failed pages)
        assert len(result) == 2
        assert result[0] == str(scanned_pdf)
        assert result[1].endswith(".ocr.txt")

    @pytest.mark.asyncio
    async def test_fake_pdf_read_as_text(self, fake_pdf_text):
        """A .pdf that's actually text should create sidecar without calling LLM."""
        with patch("tutor_platform.rag.pipeline.get_llm_client") as mock_get:
            mock_client = mock_get.return_value
            mock_client.supports_multimodal_images.return_value = True
            result = await run_pipeline([str(fake_pdf_text)])
        # Fake .pdf replaced by .ocr.txt sidecar
        assert len(result) == 1
        assert result[0].endswith(".ocr.txt")
        assert os.path.isfile(result[0])
        content = Path(result[0]).read_text(encoding="utf-8")
        assert "WeChat gateway" in content
        # LLM should NOT be called — text was read directly
        mock_client.complete.assert_not_called()


class TestProcessingContext:
    """ProcessingContext data-collection and summary behaviour."""

    def test_empty_context(self):
        ctx = ProcessingContext()
        assert ctx.success
        assert "files_in=0" in ctx.summary()

    def test_context_with_errors(self):
        ctx = ProcessingContext(
            original_paths=[Path("a.pdf")],
            augmented_paths=[Path("a.pdf")],
        )
        ctx.record_error(Path("a.pdf"), "Page 3: timeout")
        assert not ctx.success
        summary = ctx.summary()
        assert "errors=[" in summary
        assert "a.pdf" in summary

    def test_sidecar_recording(self):
        ctx = ProcessingContext()
        ctx.record_sidecar(Path("a.pdf"), Path("a.ocr.txt"))
        assert ctx.stats["sidecars_created"] == 1
        assert Path("a.pdf") in ctx.sidecars
        assert ctx.sidecars[Path("a.pdf")] == [Path("a.ocr.txt")]


class TestPipelineConfig:
    """PipelineConfig env-var override behaviour."""

    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.ocr_enabled is True
        assert cfg.ocr_max_pages == 50
        assert cfg.ocr_dpi == 200

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("RAG_PIPELINE_OCR_ENABLED", "false")
        monkeypatch.setenv("RAG_PIPELINE_OCR_MAX_PAGES", "10")
        cfg = PipelineConfig.from_env()
        assert cfg.ocr_enabled is False
        assert cfg.ocr_max_pages == 10

    def test_invalid_env_ignored(self, monkeypatch):
        monkeypatch.setenv("RAG_PIPELINE_OCR_MAX_PAGES", "not-a-number")
        cfg = PipelineConfig.from_env()
        assert cfg.ocr_max_pages == 50  # stays default
