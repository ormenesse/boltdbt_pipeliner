from __future__ import annotations

import pandas as pd


def process_data(self, input_tables):
    crashes = input_tables["crashes"].copy()
    numeric_cols = [
        "persons_injured",
        "persons_killed",
        "pedestrians_injured",
        "cyclists_injured",
        "motorists_injured",
        "crash_hour",
    ]

    for col in numeric_cols:
        crashes[col] = pd.to_numeric(crashes[col], errors="coerce").fillna(0).astype("Int64")

    for col, value in {
        "borough": "UNKNOWN",
        "contributing_factor_vehicle_1": "UNSPECIFIED",
        "contributing_factor_vehicle_2": "UNSPECIFIED",
        "vehicle_type_code1": "UNKNOWN",
        "vehicle_type_code2": "UNKNOWN",
    }.items():
        crashes[col] = crashes[col].fillna(value)

    crash_dt = pd.to_datetime(crashes["crash_date"], errors="coerce")
    crashes["crash_month"] = crash_dt.dt.month.fillna(0).astype("Int64")
    crashes["day_of_week"] = crash_dt.dt.day_name().str[:3].fillna("UNK")
    crashes["is_weekend"] = crash_dt.dt.dayofweek.isin([5, 6]).astype("Int64")
    crashes["is_night"] = ((crashes["crash_hour"] >= 22) | (crashes["crash_hour"] <= 5)).astype("Int64")
    crashes["has_injury"] = (crashes["persons_injured"] > 0).astype("Int64")
    crashes["has_fatality"] = (crashes["persons_killed"] > 0).astype("Int64")
    return crashes.drop_duplicates(subset=["collision_id"])
