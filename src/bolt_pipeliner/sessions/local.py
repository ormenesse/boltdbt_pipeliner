"""Local Spark session builder. Preserves the original stub behavior of returning the
global `spark` symbol provided by the runtime (Databricks Connect, EMR, etc.).
"""

from __future__ import annotations

import logging
from typing import Any

logging.basicConfig(level=logging.INFO)


def create_session(spark_config: dict[str, Any] | None = None):
    """Return the ambient SparkSession.

    Mirrors the original `spark_session.py` behavior: in Databricks Connect / EMR
    environments, `spark` is already provided in the runtime globals. For local
    development, prefer constructing a SparkSession explicitly.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "pyspark is required for the local session. Install with `pip install bolt_pipeliner[spark]`."
        ) from exc

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    builder = SparkSession.builder.appName("bolt_pipeliner")
    for key, value in (spark_config or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
