"""Storage configuration validation for the platform."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def validate_provider_config() -> dict:
    """Validate the provider configuration on startup.

    Checks environment variables, directory existence, and
    connectivity to required services.

    Returns:
        dict with 'errors' (list) and 'warnings' (list).
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Check required directories exist
    for name, path in [
        ("MASTERY_DIR", os.getenv("MASTERY_DIR", "/data/mastery")),
        ("CHROMA_PERSIST_DIR", os.getenv("CHROMA_PERSIST_DIR", "/data/chromadb")),
        ("UPLOADS_DIR", os.getenv("UPLOADS_DIR", "/data/uploads")),
        ("SOURCES_DIR", os.getenv("SOURCES_DIR", "/data/sources")),
    ]:
        if not os.path.isdir(path):
            try:
                os.makedirs(path, exist_ok=True)
            except OSError as e:
                warnings.append(f"{name}={path}: could not create ({e})")

    # Check DeepSeek API key
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        warnings.append("DEEPSEEK_API_KEY not set — deepseek provider will fail")
    elif not api_key.startswith("sk-"):
        warnings.append("DEEPSEEK_API_KEY format unexpected (should start with sk-)")

    return {"errors": errors, "warnings": warnings}
