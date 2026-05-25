"""Pipeline configuration with env var overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import ClassVar, get_type_hints


@dataclass
class PipelineConfig:
    """Central configuration for the document classification pipeline.

    Every field can be overridden via the corresponding ``RAG_PIPELINE_*``
    environment variable (e.g. ``RAG_PIPELINE_OCR_MAX_PAGES=100``).  The env
    var wins when set to a non-empty value.
    """

    # ── OCR stage ────────────────────────────────────────────────
    ocr_enabled: bool = True
    ocr_max_pages: int = 50
    ocr_dpi: int = 200
    ocr_concurrency: int = 1        # reserved for future parallel use
    ocr_page_timeout: float = 120.0
    ocr_max_retries: int = 2
    ocr_retry_delay: float = 5.0
    meaningful_text_threshold: int = 50

    _ENV_PREFIX: ClassVar[str] = "RAG_PIPELINE_"

    # Scalar type dispatch for env-var parsing — (type, default)
    _FIELDS: ClassVar[dict[str, tuple[type, object]]] = {}

    def __post_init__(self) -> None:
        if not self._FIELDS:
            self._build_field_map()
        self._apply_env_overrides()

    def _build_field_map(self) -> None:
        hints = get_type_hints(self.__class__)
        for f in self.__dataclass_fields__:
            if f.startswith("_"):
                continue
            t = hints.get(f, str)
            self._FIELDS[f] = (t, getattr(self, f))

    def _apply_env_overrides(self) -> None:
        for name, (typ, default) in self._FIELDS.items():
            env_key = f"{self._ENV_PREFIX}{name.upper()}"
            val = os.environ.get(env_key)
            if val is None or val == "":
                continue
            try:
                if typ is bool:
                    setattr(self, name, val.lower() in ("1", "true", "yes"))
                else:
                    setattr(self, name, typ(val))
            except (ValueError, TypeError) as exc:
                import logging
                logging.getLogger(__name__).warning(
                    "Ignoring invalid %s=%r: %s", env_key, val, exc
                )

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """Build config from defaults + env overrides (convenience shortcut)."""
        return cls()
