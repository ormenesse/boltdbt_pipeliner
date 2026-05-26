"""Engine-portable data-quality check tests (Pandas + Polars).

Spark coverage is exercised in CI via the package's spark extra; this file
keeps the local test loop fast and dependency-light.
"""

import datetime as dt

import pandas as pd
import polars as pl
import pytest

from bolt_pipeliner.testing.checks import (
    freshness,
    not_null,
    row_count,
    schema,
    unique,
)
from bolt_pipeliner.testing.runner import run_checks


@pytest.fixture
def pandas_df():
    return pd.DataFrame(
        {
            "year_month": [202401, 202402, 202403],
            "account_id": [1, 2, 3],
            "amount": [10.0, 20.0, 30.0],
        }
    )


@pytest.fixture
def polars_df(pandas_df):
    return pl.from_pandas(pandas_df)


# --- not_null --------------------------------------------------------------- #

def test_not_null_passes_on_clean_pandas(pandas_df):
    r = not_null(pandas_df, ["year_month", "account_id"])
    assert r.passed
    assert r.rows_failed == 0


def test_not_null_fails_on_pandas_with_null():
    df = pd.DataFrame({"a": [1, None, 3]})
    r = not_null(df, ["a"])
    assert not r.passed
    assert r.rows_failed == 1


def test_not_null_passes_on_clean_polars(polars_df):
    r = not_null(polars_df, ["year_month", "account_id"])
    assert r.passed


def test_not_null_flags_missing_column(pandas_df):
    r = not_null(pandas_df, ["does_not_exist"])
    assert not r.passed
    assert "Missing column" in r.details


# --- unique ----------------------------------------------------------------- #

def test_unique_passes_pandas(pandas_df):
    assert unique(pandas_df, ["account_id"]).passed


def test_unique_fails_pandas_on_duplicates():
    df = pd.DataFrame({"k": [1, 1, 2]})
    r = unique(df, ["k"])
    assert not r.passed
    assert r.rows_failed == 1


def test_unique_polars(polars_df):
    assert unique(polars_df, ["account_id"]).passed


# --- row_count -------------------------------------------------------------- #

def test_row_count_pandas(pandas_df):
    assert row_count(pandas_df, min=1).passed
    assert not row_count(pandas_df, min=100).passed
    assert not row_count(pandas_df, min=1, max=2).passed


def test_row_count_polars(polars_df):
    assert row_count(polars_df, min=1, max=3).passed


# --- schema ----------------------------------------------------------------- #

def test_schema_pandas(pandas_df):
    assert schema(pandas_df, ["year_month", "account_id"]).passed
    assert not schema(pandas_df, ["missing_col"]).passed


# --- freshness -------------------------------------------------------------- #

def test_freshness_passes_recent_year_month():
    today = dt.date.today()
    ym = today.year * 100 + today.month
    df = pd.DataFrame({"year_month": [ym]})
    r = freshness(df, "year_month", max_age_days=40)
    assert r.passed


def test_freshness_fails_old_year_month():
    df = pd.DataFrame({"year_month": [200001]})
    r = freshness(df, "year_month", max_age_days=30)
    assert not r.passed


# --- runner / YAML dispatch ------------------------------------------------- #

def test_runner_executes_yaml_block(pandas_df):
    tests = [
        {"not_null": ["year_month", "account_id"]},
        {"unique": ["account_id"]},
        {"row_count": {"min": 1, "max": 10}},
        {"schema": ["year_month", "account_id", "amount"]},
    ]
    results = run_checks(pandas_df, tests)
    assert len(results) == 4
    assert all(r.passed for r in results), results


def test_runner_unknown_check_returns_failure(pandas_df):
    results = run_checks(pandas_df, [{"definitely_not_a_check": []}])
    assert len(results) == 1
    assert not results[0].passed
    assert "Unknown check" in results[0].details


def test_runner_malformed_entry_returns_failure(pandas_df):
    # multi-key dicts are malformed
    results = run_checks(pandas_df, [{"a": 1, "b": 2}])
    assert len(results) == 1
    assert not results[0].passed


def test_test_result_repr_html_contains_pass_or_fail(pandas_df):
    r = row_count(pandas_df, min=1)
    html = r._repr_html_()
    assert "PASS" in html
    assert "row_count" in html
