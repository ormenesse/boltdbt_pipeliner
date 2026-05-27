from __future__ import annotations

import pandas as pd


def _clean_text(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.where(cleaned.notna() & (cleaned != ""), pd.NA)
    return cleaned.str.upper()


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"].copy()
    result = pd.DataFrame(
        {
            "vehicle_record_id": _to_int(vehicles["unique_id"]),
            "collision_id": _to_int(vehicles["collision_id"]),
            "vehicle_id": vehicles["vehicle_id"].astype("string"),
            "vehicle_type": _clean_text(vehicles["vehicle_type"]),
            "vehicle_year": _to_int(vehicles["vehicle_year"]),
            "vehicle_occupants": _to_int(vehicles["vehicle_occupants"]),
            "driver_sex": _clean_text(vehicles["driver_sex"]),
            "pre_crash": _clean_text(vehicles["pre_crash"]),
            "point_of_impact": _clean_text(vehicles["point_of_impact"]),
            "public_property_damage": _clean_text(vehicles["public_property_damage"]),
            "contributing_factor_1": _clean_text(vehicles["contributing_factor_1"]),
            "contributing_factor_2": _clean_text(vehicles["contributing_factor_2"]),
        }
    )
    result = result[result["collision_id"].notna()].copy()
    return result.drop_duplicates(subset=["vehicle_record_id"])
