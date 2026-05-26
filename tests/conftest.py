from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest


@pytest.fixture
def write_config(tmp_path: Path):
    """Write a YAML config to a temp file and return its path."""

    def _write(content: str) -> Path:
        config_path = tmp_path / "etl_config.yaml"
        config_path.write_text(dedent(content).lstrip(), encoding="utf-8")
        return config_path

    return _write
