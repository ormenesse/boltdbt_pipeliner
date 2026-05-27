from __future__ import annotations

import pandas as pd


def _clean_text(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.where(cleaned.notna() & (cleaned != ""), pd.NA)
    return cleaned.str.upper()


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def process_data(self, input_tables):
    crashes = input_tables["crashes"].copy()
    crashes["crash_date"] = pd.to_datetime(crashes["crash_date"], errors="coerce")

    result = pd.DataFrame(
        {
            "collision_id": _to_int(crashes["collision_id"]),
            "crash_date": crashes["crash_date"],
            "crash_time": crashes["crash_time"].astype("string"),
            "borough": _clean_text(crashes["borough"]),
            "zip_code": crashes["zip_code"].astype("string"),
            "latitude": pd.to_numeric(crashes["latitude"], errors="coerce"),
            "longitude": pd.to_numeric(crashes["longitude"], errors="coerce"),
            "persons_injured": _to_int(crashes["number_of_persons_injured"]),
            "persons_killed": _to_int(crashes["number_of_persons_killed"]),
            "pedestrians_injured": _to_int(crashes["number_of_pedestrians_injured"]),
            "cyclists_injured": _to_int(crashes["number_of_cyclist_injured"]),
            "motorists_injured": _to_int(crashes["number_of_motorist_injured"]),
            "contributing_factor_vehicle_1": _clean_text(crashes["contributing_factor_vehicle_1"]),
            "contributing_factor_vehicle_2": _clean_text(crashes["contributing_factor_vehicle_2"]),
            "vehicle_type_code1": _clean_text(crashes["vehicle_type_code1"]),
            "vehicle_type_code2": _clean_text(crashes["vehicle_type_code2"]),
        }
    )

    result = result[result["collision_id"].notna()].copy()
    result["year_month"] = result["crash_date"].dt.strftime("%Y%m").astype("Int64")
    result["crash_hour"] = (
        pd.to_numeric(result["crash_time"].fillna("").str.split(":").str[0], errors="coerce")
        .astype("Int64")
    )
    return result
