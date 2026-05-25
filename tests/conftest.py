"""Shared fixtures for the platform test suite."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_llm_client() -> MagicMock:
    """A mock LLM client that advertises multimodal support.

    The ``complete`` method returns a canned OCR transcription.
    """
    client = MagicMock()
    client.supports_multimodal_images.return_value = True
    client.config.binding = "ollama"
    client.config.model = "minicpm-v4.6"
    client.complete = AsyncMock(return_value="Mocked OCR text content.")
    return client
