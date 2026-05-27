from __future__ import annotations

from pyspark.sql import functions as F


def _count_when(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("int")


def process_data(self, input_tables):
    persons = input_tables["persons"]
    role_text = F.concat_ws(" ", "person_type", "ped_role")

    return persons.groupBy("collision_id").agg(
        F.count("*").cast("int").alias("person_count"),
        _count_when(F.col("person_type") == "OCCUPANT").alias("occupant_count"),
        _count_when(role_text.rlike("PEDESTRIAN")).alias("pedestrian_person_count"),
        _count_when(role_text.rlike("BICYCLIST|CYCLIST")).alias("cyclist_person_count"),
        _count_when(F.col("person_age") < 18).alias("minor_count"),
        _count_when(F.col("person_age") >= 65).alias("senior_count"),
        F.avg("person_age").alias("avg_person_age"),
        _count_when(F.col("person_sex") == "F").alias("female_person_count"),
        _count_when(F.col("person_sex") == "M").alias("male_person_count"),
        _count_when(F.col("safety_equipment").rlike("LAP BELT|HARNESS|HELMET")).alias("safety_equipment_count"),
    )
