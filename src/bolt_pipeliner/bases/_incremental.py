from __future__ import annotations

from dataclasses import dataclass
from typing import Any

VALID_INCREMENTAL_TYPES = {"int", "date"}
VALID_DATE_GRAINS = {"yearly", "monthly", "daily"}


@dataclass(frozen=True)
class IncrementalPolicy:
    enabled: bool
    column: str
    value_type: str
    date_grain: str
    mode: str
    window_size: int | None


def normalize_date_grain(value: str | None) -> str:
    grain = (value or "monthly").strip().lower()
    aliases = {
        "year": "yearly",
        "month": "monthly",
        "day": "daily",
    }
    grain = aliases.get(grain, grain)
    if grain not in VALID_DATE_GRAINS:
        raise ValueError(
            "incremental_date_grain must be one of: yearly, monthly, daily."
        )
    return grain


def resolve_incremental_mode(
    unit: int | str | None,
    *,
    default_window: int,
) -> tuple[str, int | None]:
    if unit is None:
        unit = default_window

    if isinstance(unit, str):
        raw = unit.strip().lower()
        if raw == "append":
            return "append", None
        if raw == "overwrite":
            return "overwrite", None
        try:
            unit = int(raw)
        except ValueError as exc:
            raise ValueError(
                "incremental_unit must be an integer or one of: append, overwrite."
            ) from exc

    if not isinstance(unit, int):
        raise ValueError(
            "incremental_unit must be an integer or one of: append, overwrite."
        )

    if unit == -1:
        return "overwrite", None
    if unit == 0:
        return "append", None
    if unit > 0:
        return "window", unit
    raise ValueError("incremental_unit cannot be lower than -1.")


def build_incremental_policy(
    *,
    enabled: bool,
    column: str,
    unit: int | str | None,
    value_type: str | None,
    date_grain: str | None,
    default_window: int,
    default_value_type: str,
    default_date_grain: str,
) -> IncrementalPolicy:
    if not column:
        raise ValueError("incremental_column must be provided.")

    resolved_type = (value_type or default_value_type).strip().lower()
    if resolved_type not in VALID_INCREMENTAL_TYPES:
        raise ValueError("incremental_type must be either 'int' or 'date'.")

    resolved_grain = normalize_date_grain(date_grain or default_date_grain)
    mode, window_size = resolve_incremental_mode(unit, default_window=default_window)

    return IncrementalPolicy(
        enabled=enabled,
        column=column,
        value_type=resolved_type,
        date_grain=resolved_grain,
        mode=mode,
        window_size=window_size,
    )


def _normalize_pandas_incremental_series(
    series,
    *,
    value_type: str,
    date_grain: str,
    column: str,
    frame_name: str,
):
    import pandas as pd

    if value_type == "int":
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = series.notna()
        invalid = non_null & (numeric.isna() | ((numeric % 1) != 0))
        if invalid.any():
            raise ValueError(
                f"Incremental column '{column}' in {frame_name} must contain integer values."
            )
        return numeric.astype("Int64")

    parsed = pd.to_datetime(series, errors="coerce")
    non_null = series.notna()
    invalid = non_null & parsed.isna()
    if invalid.any():
        raise ValueError(
            f"Incremental column '{column}' in {frame_name} must contain parseable dates."
        )

    normalized = parsed.dt.floor("D")
    if date_grain == "yearly":
        valid_grain = normalized.dt.month.eq(1) & normalized.dt.day.eq(1)
    elif date_grain == "monthly":
        valid_grain = normalized.dt.day.eq(1)
    else:
        valid_grain = ~normalized.isna()

    bad_grain = non_null & (~valid_grain)
    if bad_grain.any():
        raise ValueError(
            f"Incremental column '{column}' in {frame_name} must follow {date_grain} date granularity."
        )

    return normalized


def apply_incremental_policy_pandas(
    existing_df,
    incoming_df,
    policy: IncrementalPolicy,
):
    import pandas as pd

    if not policy.enabled or policy.mode == "overwrite":
        return incoming_df.copy()

    if policy.column not in incoming_df.columns:
        raise ValueError(
            f"Incremental column '{policy.column}' not found in processed DataFrame."
        )

    marker = "__bp_incremental_value"
    incoming = incoming_df.copy()
    incoming[marker] = _normalize_pandas_incremental_series(
        incoming[policy.column],
        value_type=policy.value_type,
        date_grain=policy.date_grain,
        column=policy.column,
        frame_name="incoming DataFrame",
    )

    if existing_df is None or existing_df.empty:
        return incoming.drop(columns=[marker])

    if policy.column not in existing_df.columns:
        raise ValueError(
            f"Incremental column '{policy.column}' not found in existing target table."
        )

    existing = existing_df.copy()
    existing[marker] = _normalize_pandas_incremental_series(
        existing[policy.column],
        value_type=policy.value_type,
        date_grain=policy.date_grain,
        column=policy.column,
        frame_name="existing target table",
    )

    existing_values = [v for v in existing[marker].dropna().unique().tolist()]
    if policy.mode == "append":
        existing_set = set(existing_values)
        incoming_values = set(incoming[marker].dropna().unique().tolist())
        values_to_append = list(incoming_values - existing_set)
        incoming_filtered = incoming[incoming[marker].isin(values_to_append)]
        merged = pd.concat(
            [existing.drop(columns=[marker]), incoming_filtered.drop(columns=[marker])],
            ignore_index=True,
        )
        return merged

    if not existing_values:
        return incoming.drop(columns=[marker])

    sorted_values = sorted(existing_values, reverse=True)
    latest_values = sorted_values[: policy.window_size or 0]
    if not latest_values:
        return incoming.drop(columns=[marker])

    cutoff = latest_values[-1]
    incoming_recent = incoming[incoming[marker] >= cutoff]
    existing_retained = existing[~existing[marker].isin(latest_values)]
    merged = pd.concat(
        [existing_retained.drop(columns=[marker]), incoming_recent.drop(columns=[marker])],
        ignore_index=True,
    )
    return merged


def incremental_values_desc(values: list[Any]) -> list[Any]:
    return sorted(values, reverse=True)
