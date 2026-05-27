from __future__ import annotations

import polars as pl


def process_data(self, input_tables):
    persons = input_tables["persons"]
    role_text = pl.concat_str([pl.col("person_type"), pl.col("ped_role")], separator=" ")
    return persons.group_by("collision_id").agg(
        pl.len().cast(pl.Int64).alias("person_count"),
        (pl.col("person_type") == "OCCUPANT").sum().cast(pl.Int64).alias("occupant_count"),
        role_text.str.contains("PEDESTRIAN").sum().cast(pl.Int64).alias("pedestrian_person_count"),
        role_text.str.contains("BICYCLIST|CYCLIST").sum().cast(pl.Int64).alias("cyclist_person_count"),
        (pl.col("person_age") < 18).sum().cast(pl.Int64).alias("minor_count"),
        (pl.col("person_age") >= 65).sum().cast(pl.Int64).alias("senior_count"),
        pl.col("person_age").mean().alias("avg_person_age"),
        (pl.col("person_sex") == "F").sum().cast(pl.Int64).alias("female_person_count"),
        (pl.col("person_sex") == "M").sum().cast(pl.Int64).alias("male_person_count"),
        pl.col("safety_equipment")
        .str.contains("LAP BELT|HARNESS|HELMET")
        .sum()
        .cast(pl.Int64)
        .alias("safety_equipment_count"),
    )
