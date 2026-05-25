"""Platform extraction hook: monkey-patch deeptutor's extract_text_from_bytes.

Extends document extraction with platform-specific capabilities:
  - Old .doc format via antiword CLI
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path

from deeptutor.utils.document_extractor import (
    EmptyDocumentError,
    DocumentTooLargeError,
    MAX_DOC_BYTES,
    extract_text_from_bytes as _original_extract,
)

logger = logging.getLogger(__name__)


def _extract_doc_bytes(data: bytes, filename: str) -> str:
    """Extract text from old .doc format via antiword CLI."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".doc", delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        result = subprocess.run(
            ["antiword", tmp_path],
            capture_output=True,
            text=True,
            timeout=30,
        )
        Path(tmp_path).unlink(missing_ok=True)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
        logger.warning("antiword failed for %s: %s", filename, result.stderr)
    except FileNotFoundError:
        logger.warning("antiword not installed, cannot extract .doc: %s", filename)
    except subprocess.TimeoutExpired:
        logger.warning("antiword timed out on: %s", filename)
    except Exception as exc:
        logger.warning("antiword error on %s: %s", filename, exc)
    return ""


def _patched_extract(filename: str, data: bytes, **kwargs) -> str:
    """Patched extract with .doc support via antiword."""
    ext = Path(filename).suffix.lower()
    if ext == ".doc" and not data.startswith(b"PK\x03\x04"):  # PK = real .docx
        if not data:
            raise EmptyDocumentError(f"{filename} is empty", filename=filename)
        max_bytes = kwargs.get("max_bytes", MAX_DOC_BYTES)
        if max_bytes is not None and len(data) > max_bytes:
            raise DocumentTooLargeError(
                f"{filename} exceeds limit", filename=filename
            )
        text = _extract_doc_bytes(data, filename)
        if text.strip():
            return text
    # Pass through to original (will raise UnsupportedDocumentError for .doc if antiword fails)
    return _original_extract(filename, data, **kwargs)


# Apply the monkey-patch
import deeptutor.utils.document_extractor as doc_mod

doc_mod.extract_text_from_bytes = _patched_extract
logger.info("Platform extraction hook applied: .doc support via antiword")
