from __future__ import annotations

from pyspark.sql import functions as F


def process_data(self, input_tables):
    persons = input_tables["persons"]
    sane_age = (F.col("person_age") >= 0) & (F.col("person_age") <= 100)

    return (
        persons.withColumn("person_age", F.when(sane_age, F.col("person_age")))
        .withColumn("person_type", F.coalesce("person_type", F.lit("UNKNOWN")))
        .withColumn("ped_role", F.coalesce("ped_role", F.lit("UNKNOWN")))
        .withColumn("person_sex", F.coalesce("person_sex", F.lit("UNKNOWN")))
        .withColumn("safety_equipment", F.coalesce("safety_equipment", F.lit("UNKNOWN")))
        .withColumn("ejection", F.coalesce("ejection", F.lit("UNKNOWN")))
    )
