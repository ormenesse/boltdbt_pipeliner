from __future__ import annotations

from pyspark.sql import functions as F


def process_data(self, input_tables):
    crashes = input_tables["crashes"]
    numeric_cols = [
        "persons_injured",
        "persons_killed",
        "pedestrians_injured",
        "cyclists_injured",
        "motorists_injured",
        "crash_hour",
    ]

    return (
        crashes.fillna(0, subset=numeric_cols)
        .fillna(
            {
                "borough": "UNKNOWN",
                "contributing_factor_vehicle_1": "UNSPECIFIED",
                "contributing_factor_vehicle_2": "UNSPECIFIED",
                "vehicle_type_code1": "UNKNOWN",
                "vehicle_type_code2": "UNKNOWN",
            }
        )
        .withColumn("crash_month", F.month("crash_date"))
        .withColumn("day_of_week", F.date_format("crash_date", "E"))
        .withColumn("is_weekend", F.dayofweek("crash_date").isin([1, 7]).cast("int"))
        .withColumn("is_night", ((F.col("crash_hour") >= 22) | (F.col("crash_hour") <= 5)).cast("int"))
        .withColumn("has_injury", (F.col("persons_injured") > 0).cast("int"))
        .withColumn("has_fatality", (F.col("persons_killed") > 0).cast("int"))
        .dropDuplicates(["collision_id"])
    )
