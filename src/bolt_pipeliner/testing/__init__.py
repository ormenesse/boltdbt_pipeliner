"""DBT-style data-quality tests for bolt_pipeliner pipelines.

User pipelines declare checks under a `tests:` block on each job in
etl_config.yaml. Built-in checks (`not_null`, `unique`, `row_count`,
`freshness`, `schema`) work uniformly across Spark / Pandas / Polars
DataFrames via duck-typed dispatch in `checks.py`.

CLI entry point:  `bolt test [--layer L] [--module M]`
Notebook helper:  `bolt_pipeliner.testing.run_for_dataframe(df, tests)`
"""

from bolt_pipeliner.testing.checks import (
    TestResult,
    freshness,
    not_null,
    row_count,
    schema,
    unique,
)
from bolt_pipeliner.testing.runner import run_checks

__all__ = [
    "TestResult",
    "freshness",
    "not_null",
    "row_count",
    "schema",
    "unique",
    "run_checks",
]
