from __future__ import annotations

from pyspark.sql import functions as F


def _clean_text(name: str):
    value = F.trim(F.col(name).cast("string"))
    return F.when(value.isNull() | (value == ""), None).otherwise(F.upper(value))


def process_data(self, input_tables):
    persons = input_tables["persons"]

    return (
        persons.select(
            F.col("unique_id").cast("long").alias("person_record_id"),
            F.col("collision_id").cast("long").alias("collision_id"),
            F.col("person_id").cast("string").alias("person_id"),
            _clean_text("person_type").alias("person_type"),
            F.col("person_age").cast("int").alias("person_age"),
            _clean_text("ejection").alias("ejection"),
            _clean_text("safety_equipment").alias("safety_equipment"),
            _clean_text("ped_role").alias("ped_role"),
            _clean_text("contributing_factor_1").alias("contributing_factor_1"),
            _clean_text("contributing_factor_2").alias("contributing_factor_2"),
            _clean_text("person_sex").alias("person_sex"),
        )
        .filter(F.col("collision_id").isNotNull())
        .dropDuplicates(["person_record_id"])
    )
