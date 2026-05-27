from __future__ import annotations

from pyspark.sql import functions as F


def _clean_text(name: str):
    value = F.trim(F.col(name).cast("string"))
    return F.when(value.isNull() | (value == ""), None).otherwise(F.upper(value))


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]

    return (
        vehicles.select(
            F.col("unique_id").cast("long").alias("vehicle_record_id"),
            F.col("collision_id").cast("long").alias("collision_id"),
            F.col("vehicle_id").cast("string").alias("vehicle_id"),
            _clean_text("vehicle_type").alias("vehicle_type"),
            F.col("vehicle_year").cast("int").alias("vehicle_year"),
            F.col("vehicle_occupants").cast("int").alias("vehicle_occupants"),
            _clean_text("driver_sex").alias("driver_sex"),
            _clean_text("pre_crash").alias("pre_crash"),
            _clean_text("point_of_impact").alias("point_of_impact"),
            _clean_text("public_property_damage").alias("public_property_damage"),
            _clean_text("contributing_factor_1").alias("contributing_factor_1"),
            _clean_text("contributing_factor_2").alias("contributing_factor_2"),
        )
        .filter(F.col("collision_id").isNotNull())
        .dropDuplicates(["vehicle_record_id"])
    )
