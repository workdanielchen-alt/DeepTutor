"""Unified file classification and preprocessing pipeline for knowledge base ingestion.

Both WeChat uploads and Web UI uploads route through this pipeline
before reaching DeepTutor's chunking + embedding + indexing stages.

Current stages:
  1. OCR scanned PDFs — detect pages without a text layer and transcribe
     them via the configured multimodal LLM (e.g. MiniCPM-v4.6),
     writing ``.ocr.txt`` sidecars that DeepTutor indexes as normal text.

The pipeline returns a (possibly augmented) list of file paths for
DeepTutor's ``document_loader`` to consume.  No DeepTutor internals
are modified — the platform simply prepares the files.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from pathlib import Path

from deeptutor.services.llm.client import get_llm_client

try:
    import fitz
except ImportError:
    fitz = None

from tutor_platform.rag.config import PipelineConfig
from tutor_platform.rag.context import ProcessingContext

logger = logging.getLogger(__name__)

_OCR_PROMPT = (
    "Transcribe all visible text from this document page image exactly as written, "
    "preserving the original language, paragraphs, headers, footnotes and line breaks. "
    "If the page contains mathematical formulas, diagrams or tables, describe their "
    "content and structure in words. Return only the transcribed text, no commentary."
)


class RagDocumentPipeline:
    """Platform-level document classification and preprocessing pipeline.

    Usage::

        file_paths = await RagDocumentPipeline.process(file_paths)
        # → file paths with OCR .txt sidecars added
    """

    _config: PipelineConfig | None = None

    # ── Public API ───────────────────────────────────────────────

    @classmethod
    async def process(cls, file_paths: list[str]) -> list[str]:
        """Run all pipeline stages and return the augmented file list."""
        if not file_paths:
            return file_paths

        ctx = ProcessingContext(
            original_paths=[Path(p) for p in file_paths],
        )

        ctx.augmented_paths = list(ctx.original_paths)
        await cls._stage_ocr_scanned_pdfs(ctx)

        # Structured summary log
        logger.info(ctx.summary())

        return [str(p) for p in ctx.augmented_paths]

    # ── Config ───────────────────────────────────────────────────

    @classmethod
    def get_config(cls) -> PipelineConfig:
        if cls._config is None:
            cls._config = PipelineConfig.from_env()
        return cls._config

    @classmethod
    def set_config(cls, config: PipelineConfig) -> None:
        cls._config = config

    # ── Stage: OCR scanned PDFs ──────────────────────────────────

    @classmethod
    async def _stage_ocr_scanned_pdfs(cls, ctx: ProcessingContext) -> None:
        """Detect scanned PDFs, OCR them, and append ``.txt`` sidecar paths."""
        config = cls.get_config()
        if not config.ocr_enabled:
            return

        pdf_paths = [p for p in ctx.augmented_paths if p.suffix.lower() == ".pdf"]
        if not pdf_paths:
            return

        # ── First pass: handle fake PDFs (gateway OCR text saved as .pdf) ──
        # Even without PyMuPDF, we can read text-based "PDFs" directly.
        remaining: list[Path] = []
        for pdf_path in pdf_paths:
            text = cls._try_read_as_text(pdf_path)
            if text is not None:
                logger.info(
                    "Non-PDF file %s read as text (%d chars, replacing in index list)",
                    pdf_path.name, len(text),
                )
                cls._write_sidecar(pdf_path, text, ctx)
                txt_path = pdf_path.with_name(pdf_path.stem + ".ocr.txt")
                try:
                    idx = ctx.augmented_paths.index(pdf_path)
                    ctx.augmented_paths[idx] = txt_path
                except ValueError:
                    pass
            else:
                remaining.append(pdf_path)

        cls._deduplicate(ctx)
        if not remaining:
            return

        # ── Second pass: real PDFs need PyMuPDF + multimodal LLM ──
        llm_client = get_llm_client()
        if not llm_client.supports_multimodal_images():
            logger.info(
                "OCR stage skipped: LLM does not support multimodal images "
                "(binding=%s, model=%s)",
                llm_client.config.binding,
                llm_client.config.model,
            )
            return

        if fitz is None:
            logger.info("OCR stage skipped: pymupdf not installed")
            return

        for pdf_path in remaining:
            await cls._ocr_single_pdf(pdf_path, config, llm_client, ctx)

        cls._deduplicate(ctx)

    @classmethod
    def _deduplicate(cls, ctx: ProcessingContext) -> None:
        """Remove duplicate paths while preserving order."""
        seen: set[Path] = set()
        deduped: list[Path] = []
        for p in ctx.augmented_paths:
            if p not in seen:
                seen.add(p)
                deduped.append(p)
        ctx.augmented_paths = deduped

    @classmethod
    async def _ocr_single_pdf(
        cls,
        pdf_path: Path,
        config: PipelineConfig,
        llm_client,
        ctx: ProcessingContext,
    ) -> None:
        """OCR a single PDF — only pages without an existing text layer."""
        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            # File might be a text-based "PDF" from gateway auto-process-media
            # that already did OCR.  Try to read it as UTF-8 text.
            text = cls._try_read_as_text(pdf_path)
            if text is not None:
                logger.info(
                    "Non-PDF file %s read as text (%d chars, replacing in index list)",
                    pdf_path.name, len(text),
                )
                cls._write_sidecar(pdf_path, text, ctx)
                # Replace fake .pdf entry with the sidecar so LlamaIndex
                # doesn't attempt to open an invalid PDF.
                txt_path = pdf_path.with_name(pdf_path.stem + ".ocr.txt")
                try:
                    idx = ctx.augmented_paths.index(pdf_path)
                    ctx.augmented_paths[idx] = txt_path
                except ValueError:
                    pass
                return
            logger.error("Failed to open PDF for OCR: %s", exc)
            ctx.record_error(pdf_path, f"Cannot open: {exc}")
            return

        ctx.stats["pdfs_scanned"] += 1
        total = len(doc)

        # Identify which pages need OCR
        text_pages: dict[int, str] = {}  # page_idx → native text
        scan_pages: list[int] = []

        for i in range(min(total, config.ocr_max_pages)):
            text = doc[i].get_text()
            if text and len(text.strip()) > config.meaningful_text_threshold:
                text_pages[i] = text.strip()
                ctx.stats["pages_skipped_textlayer"] += 1
            else:
                scan_pages.append(i)

        if not scan_pages:
            # All pages have a text layer — nothing to OCR
            doc.close()
            ctx.stats["pdfs_skipped_textlayer"] += 1
            return

        logger.info(
            "OCR %d/%d pages of %s (%d already have text layer)",
            len(scan_pages), total, pdf_path.name, len(text_pages),
        )

        # OCR each page that needs it
        pages: list[str] = []
        try:
            for idx in scan_pages:
                text = await cls._ocr_page_with_retry(
                    doc, idx, pdf_path, config, llm_client, ctx,
                )
                pages.append(text)
        finally:
            doc.close()

        # Build combined output: use native text for pages that have it,
        # OCR transcription for pages that don't.
        combined_parts: list[str] = []
        for i in range(min(total, config.ocr_max_pages)):
            if i in text_pages:
                combined_parts.append(f"--- Page {i+1} ---\n{text_pages[i]}")
            else:
                # Find the OCR result for this page (in same order as scan_pages)
                idx_in_scan = scan_pages.index(i) if i in scan_pages else -1
                if idx_in_scan >= 0 and idx_in_scan < len(pages):
                    combined_parts.append(f"--- Page {i+1} ---\n{pages[idx_in_scan]}")
        # Pages beyond ocr_max_pages are omitted
        ocr_text = "\n\n".join(combined_parts) if combined_parts else ""

        if not ocr_text.strip():
            logger.warning("OCR produced empty output for %s", pdf_path.name)
            return

        cls._write_sidecar(pdf_path, ocr_text, ctx)
        logger.info(
            "OCR complete for %s -> %s (%d chars)",
            pdf_path.name,
            pdf_path.with_name(pdf_path.stem + ".ocr.txt").name,
            len(ocr_text),
        )

    # ── Helpers: text fallback & sidecar write ───────────────────

    @classmethod
    def _try_read_as_text(cls, path: Path) -> str | None:
        """Try to read a file as UTF-8 text.

        Returns the text if successful, ``None`` if the file is binary
        (e.g. a real PDF that happened to trigger a transient open error).
        """
        try:
            data = path.read_bytes()
        except OSError:
            return None

        # Reject real PDFs (start with %PDF-), even if they're valid UTF-8
        if data.startswith(b"%PDF-"):
            return None

        try:
            text = data.decode("utf-8")
            stripped = text.strip()
            if len(stripped) >= cls.get_config().meaningful_text_threshold:
                return stripped
            return None
        except (UnicodeDecodeError, OSError):
            return None

    @classmethod
    def _write_sidecar(cls, pdf_path: Path, content: str, ctx: ProcessingContext) -> None:
        """Atomically write an ``.ocr.txt`` sidecar and insert it into augmented paths."""
        txt_path = pdf_path.with_name(pdf_path.stem + ".ocr.txt")
        tmp_path = txt_path.with_name(txt_path.name + ".tmp")
        try:
            tmp_path.write_text(content, encoding="utf-8")
            os.replace(str(tmp_path), str(txt_path))
        except OSError as exc:
            logger.error("Failed to write sidecar for %s: %s", pdf_path.name, exc)
            ctx.record_error(pdf_path, f"Cannot write sidecar: {exc}")
            return

        # Insert sidecar right after the source PDF so related files cluster
        try:
            idx = ctx.augmented_paths.index(pdf_path)
            ctx.augmented_paths.insert(idx + 1, txt_path)
        except ValueError:
            ctx.augmented_paths.append(txt_path)

        ctx.record_sidecar(pdf_path, txt_path)

    # ── Per-page OCR with retry ──────────────────────────────────

    @classmethod
    async def _ocr_page_with_retry(
        cls,
        doc,
        page_idx: int,
        pdf_path: Path,
        config: PipelineConfig,
        llm_client,
        ctx: ProcessingContext,
    ) -> str:
        """OCR a single page with timeout and retry."""
        last_error: Exception | None = None

        for attempt in range(1 + config.ocr_max_retries):
            try:
                return await asyncio.wait_for(
                    cls._ocr_page(doc, page_idx, pdf_path, llm_client),
                    timeout=config.ocr_page_timeout,
                )
            except asyncio.TimeoutError:
                last_error = TimeoutError(
                    f"Page {page_idx + 1} timeout ({config.ocr_page_timeout}s)"
                )
                logger.warning(
                    "Timeout OCR page %d/%d of %s (attempt %d/%d)",
                    page_idx + 1, len(doc), pdf_path.name,
                    attempt + 1, 1 + config.ocr_max_retries,
                )
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Error OCR page %d/%d of %s (attempt %d/%d): %s",
                    page_idx + 1, len(doc), pdf_path.name,
                    attempt + 1, 1 + config.ocr_max_retries, exc,
                )

            if attempt < config.ocr_max_retries:
                await asyncio.sleep(config.ocr_retry_delay)

        # All retries exhausted
        ctx.stats["pages_failed"] += 1
        if not ctx.errors.get(pdf_path):
            ctx.errors[pdf_path] = ""
        ctx.errors[pdf_path] += (
            f" Page {page_idx + 1}: {last_error}" if ctx.errors[pdf_path]
            else f"Page {page_idx + 1}: {last_error}"
        )
        # Return empty string for this page so other pages still get included
        return ""

    @classmethod
    async def _ocr_page(
        cls,
        doc,
        page_idx: int,
        pdf_path: Path,
        llm_client,
    ) -> str:
        """Render a page to image and send to the multimodal LLM."""
        page = doc[page_idx]
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("png")
        img_b64 = base64.b64encode(img_bytes).decode("ascii")

        response = await llm_client.complete(
            _OCR_PROMPT,
            image_data=img_b64,
            image_mime_type="image/png",
            image_filename=f"{pdf_path.name}:page{page_idx + 1}",
        )
        return response.strip()

    # ── Text-layer detection (kept for backward compat) ──────────

    @classmethod
    def _has_text_layer(cls, pdf_path: Path) -> bool:
        """Return True iff the PDF contains meaningful extracted text.

        .. deprecated::
           Kept for external callers.  The pipeline itself now uses
           per-page detection for mixed-PDF support.
        """
        try:
            doc = fitz.open(pdf_path)
        except Exception:
            return False
        try:
            for page in doc:
                text = page.get_text()
                threshold = cls.get_config().meaningful_text_threshold
                if text and len(text.strip()) > threshold:
                    return True
            return False
        finally:
            doc.close()
