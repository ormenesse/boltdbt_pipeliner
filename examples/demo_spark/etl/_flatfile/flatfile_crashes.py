from __future__ import annotations

from pyspark.sql import functions as F


def _clean_text(name: str):
    value = F.trim(F.col(name).cast("string"))
    return F.when(value.isNull() | (value == ""), None).otherwise(F.upper(value))


def process_data(self, input_tables):
    crashes = input_tables["crashes"]
    crash_hour = F.split(F.col("crash_time"), ":").getItem(0).cast("int")

    return (
        crashes.select(
            F.col("collision_id").cast("long").alias("collision_id"),
            F.to_date("crash_date").alias("crash_date"),
            F.col("crash_time").cast("string").alias("crash_time"),
            _clean_text("borough").alias("borough"),
            F.col("zip_code").cast("string").alias("zip_code"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("number_of_persons_injured").cast("int").alias("persons_injured"),
            F.col("number_of_persons_killed").cast("int").alias("persons_killed"),
            F.col("number_of_pedestrians_injured").cast("int").alias("pedestrians_injured"),
            F.col("number_of_cyclist_injured").cast("int").alias("cyclists_injured"),
            F.col("number_of_motorist_injured").cast("int").alias("motorists_injured"),
            _clean_text("contributing_factor_vehicle_1").alias("contributing_factor_vehicle_1"),
            _clean_text("contributing_factor_vehicle_2").alias("contributing_factor_vehicle_2"),
            _clean_text("vehicle_type_code1").alias("vehicle_type_code1"),
            _clean_text("vehicle_type_code2").alias("vehicle_type_code2"),
        )
        .withColumn("year_month", F.date_format("crash_date", "yyyyMM").cast("int"))
        .withColumn("crash_hour", crash_hour)
        .filter(F.col("collision_id").isNotNull())
    )
