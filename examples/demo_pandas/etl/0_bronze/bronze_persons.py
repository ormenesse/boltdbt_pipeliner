from __future__ import annotations

import pandas as pd


def process_data(self, input_tables):
    persons = input_tables["persons"].copy()
    age = pd.to_numeric(persons["person_age"], errors="coerce")
    persons["person_age"] = age.where(age.between(0, 100), pd.NA).astype("Float64")

    for col in ["person_type", "ped_role", "person_sex", "safety_equipment", "ejection"]:
        persons[col] = persons[col].fillna("UNKNOWN")

    return persons
