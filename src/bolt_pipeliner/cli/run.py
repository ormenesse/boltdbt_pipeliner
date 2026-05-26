from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

from bolt_pipeliner.runner import run as runner_run


def execute(
    config_path: Path,
    layers: Optional[Iterable[str]] = None,
    select: Optional[str] = None,
    layer: Optional[str] = None,
) -> None:
    runner_run(config_path, layers=layers, select=select, layer=layer)
