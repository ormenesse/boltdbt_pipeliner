from __future__ import annotations

import importlib
from typing import Any


def create_session(profile: str = "local", spark_config: dict[str, Any] | None = None) -> Any:
    """Dispatch to the matching session module. Cloud SDKs are imported lazily."""
    module = importlib.import_module(f"bolt_pipeliner.sessions.{profile}")
    return module.create_session(spark_config or {})


__all__ = ["create_session"]
