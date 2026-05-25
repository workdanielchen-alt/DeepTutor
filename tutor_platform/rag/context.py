"""Processing context for the document pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ProcessingContext:
    """Collects results, errors and statistics during pipeline execution.

    A single instance is created per ``RagDocumentPipeline.process()`` call
    and passed through all stages.
    """

    # ── Input / output ───────────────────────────────────────────
    original_paths: list[Path] = field(default_factory=list)
    augmented_paths: list[Path] = field(default_factory=list)

    # ── Per-file tracking ────────────────────────────────────────
    sidecars: dict[Path, list[Path]] = field(default_factory=dict)
    errors: dict[Path, str] = field(default_factory=dict)

    # ── Counters ─────────────────────────────────────────────────
    stats: dict[str, int] = field(default_factory=lambda: {
        "pdfs_scanned": 0,
        "pdfs_skipped_textlayer": 0,
        "pdfs_failed": 0,
        "pages_ocrd": 0,
        "pages_skipped_textlayer": 0,
        "pages_failed": 0,
        "sidecars_created": 0,
    })

    # ── Helpers ──────────────────────────────────────────────────

    def record_sidecar(self, pdf: Path, sidecar: Path) -> None:
        self.sidecars.setdefault(pdf, []).append(sidecar)
        self.stats["sidecars_created"] += 1

    def record_error(self, pdf: Path, msg: str) -> None:
        self.errors[pdf] = msg
        self.stats["pdfs_failed"] += 1

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        parts = [f"files_in={len(self.original_paths)}",
                 f"files_out={len(self.augmented_paths)}",
                 f"sidecars={self.stats.get('sidecars_created', 0)}",
                 f"pdfs_scanned={self.stats.get('pdfs_scanned', 0)}",
                 f"pages_ocrd={self.stats.get('pages_ocrd', 0)}",
                 f"pages_skipped_textlayer={self.stats.get('pages_skipped_textlayer', 0)}",
                 f"pages_failed={self.stats.get('pages_failed', 0)}"]
        if self.errors:
            err_str = ", ".join(f"{p.name}: {msg}" for p, msg in self.errors.items())
            parts.append(f"errors=[{err_str}]")
        return "RagDocumentPipeline summary: " + ", ".join(parts)
