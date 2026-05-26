"""Engine-agnostic data quality checks.

Each check accepts a DataFrame (Spark / Pandas / Polars) plus parameters and
returns a TestResult. Dispatch happens by `isinstance` so the user's job code
doesn't need to know which engine produced the DataFrame.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class TestResult:
    name: str
    passed: bool
    details: str
    rows_failed: Optional[int] = None

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        extra = f" ({self.rows_failed} rows failing)" if self.rows_failed else ""
        return f"<{status} {self.name}{extra}: {self.details}>"

    def _repr_html_(self) -> str:
        color = "#22c55e" if self.passed else "#ef4444"
        status = "PASS" if self.passed else "FAIL"
        return (
            f"<div style='font-family: monospace; padding: 6px 10px; "
            f"border-left: 4px solid {color}; margin: 4px 0;'>"
            f"<b style='color:{color}'>{status}</b> &nbsp; {self.name}"
            f"<br/><small>{self.details}</small></div>"
        )


# --------------------------------------------------------------------------- #
# Engine detection (duck-typed; no hard imports)
# --------------------------------------------------------------------------- #

def _is_spark(df: Any) -> bool:
    return type(df).__module__.startswith("pyspark.")


def _is_polars(df: Any) -> bool:
    return type(df).__module__.startswith("polars.")


def _is_pandas(df: Any) -> bool:
    return type(df).__module__.startswith("pandas.")


def _row_count(df: Any) -> int:
    if _is_spark(df):
        return df.count()
    if _is_polars(df):
        return df.height
    if _is_pandas(df):
        return len(df.index)
    raise TypeError(f"Unsupported DataFrame type: {type(df)!r}")


def _columns(df: Any) -> list[str]:
    if _is_spark(df):
        return list(df.columns)
    if _is_polars(df):
        return df.columns
    if _is_pandas(df):
        return list(df.columns)
    raise TypeError(f"Unsupported DataFrame type: {type(df)!r}")


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #

def not_null(df: Any, columns: list[str]) -> TestResult:
    """Every value in `columns` is non-null."""
    name = f"not_null({columns})"
    if not columns:
        return TestResult(name, False, "Empty `columns` list")
    missing_cols = [c for c in columns if c not in _columns(df)]
    if missing_cols:
        return TestResult(name, False, f"Missing column(s): {missing_cols}")

    failing = 0
    if _is_spark(df):
        from pyspark.sql import functions as F

        cond = F.col(columns[0]).isNull()
        for c in columns[1:]:
            cond = cond | F.col(c).isNull()
        failing = df.filter(cond).count()
    elif _is_polars(df):
        import polars as pl

        expr = pl.col(columns[0]).is_null()
        for c in columns[1:]:
            expr = expr | pl.col(c).is_null()
        failing = df.filter(expr).height
    elif _is_pandas(df):
        mask = df[columns].isna().any(axis=1)
        failing = int(mask.sum())
    else:
        return TestResult(name, False, f"Unsupported DataFrame type {type(df)!r}")

    passed = failing == 0
    return TestResult(
        name,
        passed,
        f"{failing} null row(s) found" if not passed else "no nulls",
        rows_failed=failing,
    )


def unique(df: Any, columns: list[str]) -> TestResult:
    """Composite key over `columns` is unique."""
    name = f"unique({columns})"
    missing_cols = [c for c in columns if c not in _columns(df)]
    if missing_cols:
        return TestResult(name, False, f"Missing column(s): {missing_cols}")

    if _is_spark(df):
        total = df.count()
        distinct = df.select(*columns).distinct().count()
        failing = total - distinct
    elif _is_polars(df):
        total = df.height
        distinct = df.select(columns).unique().height
        failing = total - distinct
    elif _is_pandas(df):
        total = len(df.index)
        distinct = len(df[columns].drop_duplicates())
        failing = total - distinct
    else:
        return TestResult(name, False, f"Unsupported DataFrame type {type(df)!r}")

    passed = failing == 0
    return TestResult(
        name,
        passed,
        f"{failing} duplicate row(s) on key {columns}" if not passed else "key is unique",
        rows_failed=failing,
    )


def row_count(df: Any, min: int = 1, max: Optional[int] = None) -> TestResult:
    """Row count falls within [min, max]."""
    name = f"row_count(min={min}, max={max})"
    count = _row_count(df)
    if count < min:
        return TestResult(name, False, f"Got {count} rows, expected >= {min}", count)
    if max is not None and count > max:
        return TestResult(name, False, f"Got {count} rows, expected <= {max}", count)
    return TestResult(name, True, f"{count} rows")


def schema(df: Any, expected: list[str]) -> TestResult:
    """DataFrame contains every column in `expected` (extras are allowed)."""
    name = f"schema({expected})"
    actual = set(_columns(df))
    missing = [c for c in expected if c not in actual]
    if missing:
        return TestResult(name, False, f"Missing column(s): {missing}")
    return TestResult(name, True, f"All {len(expected)} expected column(s) present")


def freshness(df: Any, column: str, max_age_days: int) -> TestResult:
    """`column` (a year_month YYYYMM int or a date) is no older than max_age_days."""
    name = f"freshness({column}, max_age_days={max_age_days})"
    if column not in _columns(df):
        return TestResult(name, False, f"Missing column: {column}")

    today = dt.date.today()
    if _is_spark(df):
        from pyspark.sql import functions as F

        max_val = df.agg(F.max(F.col(column))).collect()[0][0]
    elif _is_polars(df):
        max_val = df.select(column).max().to_series()[0]
    elif _is_pandas(df):
        max_val = df[column].max()
    else:
        return TestResult(name, False, f"Unsupported DataFrame type {type(df)!r}")

    if max_val is None:
        return TestResult(name, False, "Column is empty")

    # Accept either YYYYMM int or a date-like value.
    try:
        max_int = int(max_val)
        if max_int > 190000 and max_int < 999912:
            # Treat as YYYYMM
            year, month = divmod(max_int, 100)
            max_date = dt.date(year, month, 1)
        else:
            return TestResult(
                name,
                False,
                f"Cannot interpret {max_val!r} as YYYYMM or date",
            )
    except (TypeError, ValueError):
        try:
            max_date = max_val.date() if hasattr(max_val, "date") else max_val
            if not isinstance(max_date, dt.date):
                return TestResult(name, False, f"Cannot convert {max_val!r} to date")
        except Exception as e:  # noqa: BLE001
            return TestResult(name, False, f"Cannot convert {max_val!r}: {e}")

    age = (today - max_date).days
    if age > max_age_days:
        return TestResult(
            name,
            False,
            f"Latest {column} is {max_date} ({age} days old, max {max_age_days})",
        )
    return TestResult(name, True, f"Latest {column} is {max_date} ({age} days old)")
