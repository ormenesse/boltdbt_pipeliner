from __future__ import annotations

import pandas as pd


def _clean_text(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.where(cleaned.notna() & (cleaned != ""), pd.NA)
    return cleaned.str.upper()


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def process_data(self, input_tables):
    persons = input_tables["persons"].copy()
    result = pd.DataFrame(
        {
            "person_record_id": _to_int(persons["unique_id"]),
            "collision_id": _to_int(persons["collision_id"]),
            "person_id": persons["person_id"].astype("string"),
            "person_type": _clean_text(persons["person_type"]),
            "person_age": _to_int(persons["person_age"]),
            "ejection": _clean_text(persons["ejection"]),
            "safety_equipment": _clean_text(persons["safety_equipment"]),
            "ped_role": _clean_text(persons["ped_role"]),
            "contributing_factor_1": _clean_text(persons["contributing_factor_1"]),
            "contributing_factor_2": _clean_text(persons["contributing_factor_2"]),
            "person_sex": _clean_text(persons["person_sex"]),
        }
    )
    result = result[result["collision_id"].notna()].copy()
    return result.drop_duplicates(subset=["person_record_id"])
