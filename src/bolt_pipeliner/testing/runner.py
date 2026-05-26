"""Runs the `tests:` block declared on each job in etl_config.yaml.

The YAML schema mirrors dbt's tests block:

    silver:
      - module: silver_fct_account_calls_monthly
        output_table_name: fct_account_calls_monthly
        tests:
          - not_null: [year_month, account_id]
          - unique:   [year_month, account_id]
          - row_count: { min: 1 }
          - freshness: { column: year_month, max_age_days: 90 }
          - schema:    [year_month, account_id, call_count]
"""

from __future__ import annotations

from typing import Any, Callable, Iterable

from bolt_pipeliner.testing import checks as _checks
from bolt_pipeliner.testing.checks import TestResult

_CHECK_REGISTRY: dict[str, Callable[..., TestResult]] = {
    "not_null": _checks.not_null,
    "unique": _checks.unique,
    "row_count": _checks.row_count,
    "schema": _checks.schema,
    "freshness": _checks.freshness,
}


def run_checks(df: Any, tests: Iterable[dict | str]) -> list[TestResult]:
    """Run every check in `tests` against `df` and return the result list."""
    results: list[TestResult] = []
    for spec in tests or []:
        if isinstance(spec, str):
            name, params = spec, {}
        elif isinstance(spec, dict) and len(spec) == 1:
            (name, raw_params), = spec.items()
            params = _coerce_params(name, raw_params)
        else:
            results.append(
                TestResult(
                    name=str(spec),
                    passed=False,
                    details="Malformed test entry; expected a single-key dict or a string",
                )
            )
            continue

        check = _CHECK_REGISTRY.get(name)
        if check is None:
            results.append(
                TestResult(
                    name=name,
                    passed=False,
                    details=f"Unknown check '{name}'. Known: {sorted(_CHECK_REGISTRY)}",
                )
            )
            continue

        try:
            results.append(check(df, **params))
        except Exception as e:  # noqa: BLE001
            results.append(
                TestResult(name=name, passed=False, details=f"Check raised: {e!r}")
            )

    return results


def _coerce_params(name: str, raw: Any) -> dict[str, Any]:
    """YAML representations vary by check. Examples:
      not_null: [year_month, account_id]   -> {"columns": [...]}
      unique:   [year_month, account_id]   -> {"columns": [...]}
      schema:   [year_month, ...]          -> {"expected": [...]}
      row_count: { min: 1, max: 100 }      -> {"min": 1, "max": 100}
      freshness: { column: ym, max_age_days: 90 }
    """
    if name in ("not_null", "unique"):
        return {"columns": list(raw) if isinstance(raw, (list, tuple)) else [raw]}
    if name == "schema":
        return {"expected": list(raw) if isinstance(raw, (list, tuple)) else [raw]}
    if isinstance(raw, dict):
        return raw
    return {"value": raw}


def render_html(results: list[TestResult]) -> str:
    """Inline HTML used by the notebook renderer + the CLI when --html is set."""
    if not results:
        return "<i>No tests declared.</i>"
    return "\n".join(r._repr_html_() for r in results)
