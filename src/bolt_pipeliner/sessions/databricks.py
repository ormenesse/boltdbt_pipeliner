"""Spark session for Databricks Jobs and Databricks Connect serverless."""

from __future__ import annotations

import os
from typing import Any


def create_session(spark_config: dict[str, Any] | None = None):
    """Reuse a Jobs/notebook session, or connect to Databricks serverless.

    A plain ``SparkSession.builder`` cannot always interpret the ``SPARK_REMOTE``
    value supplied to Databricks job processes. Databricks Connect's serverless
    builder handles the connection when there is no active session.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "The databricks Spark profile requires pyspark (available in Databricks "
            "runtimes or via `pip install bolt_pipeliner[spark]`)."
        ) from exc

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        if os.environ.get("SPARK_REMOTE", "").startswith("unix://"):
            raise RuntimeError(
                "No active Databricks Spark session. Install `bolt_pipeliner[spark]` "
                "for a Databricks Connect serverless session."
            ) from exc
        # Classic Jobs clusters ship PySpark but need not ship Databricks Connect.
        builder = SparkSession.builder
        for key, value in (spark_config or {}).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    builder = DatabricksSession.builder.serverless(True)
    for key, value in (spark_config or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
