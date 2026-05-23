from __future__ import annotations

import importlib.util
import io
from pathlib import Path
import sys
from unittest import mock

import pytest

_CLI_KIT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "_cli_kit.py"


def _load_cli_kit():
    spec = importlib.util.spec_from_file_location("cli_kit_under_test", _CLI_KIT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(
    not _CLI_KIT_PATH.exists(),
    reason="_cli_kit.py is generated at build time, not present in source tree",
)
def test_log_helpers_do_not_raise_on_legacy_windows_code_page() -> None:
    buffer = io.BytesIO()
    stdout = io.TextIOWrapper(buffer, encoding="cp936", errors="strict")

    with mock.patch("sys.stdout", stdout):
        cli_kit = _load_cli_kit()

        assert sys.stdout.errors == "replace"
        cli_kit.banner("DeepTutor", ["Backend http://localhost:8001"])
        cli_kit.log_success("DeepTutor started")
        cli_kit.log_error("DeepTutor failed")
        stdout.flush()

    assert buffer.getvalue()
