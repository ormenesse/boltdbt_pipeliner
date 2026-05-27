from __future__ import annotations

import pandas as pd


def process_data(self, input_tables):
    persons = input_tables["persons"].copy()
    role_text = persons["person_type"].fillna("") + " " + persons["ped_role"].fillna("")

    work = persons.assign(
        occupant_count=(persons["person_type"] == "OCCUPANT").astype(int),
        pedestrian_person_count=role_text.str.contains("PEDESTRIAN", na=False).astype(int),
        cyclist_person_count=role_text.str.contains("BICYCLIST|CYCLIST", na=False).astype(int),
        minor_count=(persons["person_age"].fillna(-1) < 18).astype(int),
        senior_count=(persons["person_age"].fillna(-1) >= 65).astype(int),
        female_person_count=(persons["person_sex"] == "F").astype(int),
        male_person_count=(persons["person_sex"] == "M").astype(int),
        safety_equipment_count=persons["safety_equipment"].fillna("").str.contains(
            "LAP BELT|HARNESS|HELMET", na=False
        ).astype(int),
    )

    grouped = (
        work.groupby("collision_id", as_index=False)
        .agg(
            person_count=("collision_id", "size"),
            occupant_count=("occupant_count", "sum"),
            pedestrian_person_count=("pedestrian_person_count", "sum"),
            cyclist_person_count=("cyclist_person_count", "sum"),
            minor_count=("minor_count", "sum"),
            senior_count=("senior_count", "sum"),
            avg_person_age=("person_age", "mean"),
            female_person_count=("female_person_count", "sum"),
            male_person_count=("male_person_count", "sum"),
            safety_equipment_count=("safety_equipment_count", "sum"),
        )
    )

    int_cols = [
        "person_count",
        "occupant_count",
        "pedestrian_person_count",
        "cyclist_person_count",
        "minor_count",
        "senior_count",
        "female_person_count",
        "male_person_count",
        "safety_equipment_count",
    ]
    grouped[int_cols] = grouped[int_cols].fillna(0).astype("Int64")
    grouped["avg_person_age"] = grouped["avg_person_age"].fillna(0.0)
    return grouped
