from __future__ import annotations

from pyspark.sql import functions as F


NUMERIC_DEFAULTS = [
    "vehicle_count",
    "passenger_car_count",
    "suv_wagon_count",
    "truck_van_count",
    "taxi_count",
    "bus_count",
    "two_wheeler_count",
    "max_vehicle_occupants",
    "property_damage_vehicle_count",
    "driver_inattention_count",
    "unsafe_speed_count",
    "failure_to_yield_count",
    "impairment_count",
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


def process_data(self, input_tables):
    crashes = input_tables["crashes"]
    vehicles = input_tables["vehicles"]
    persons = input_tables["persons"]

    joined = crashes.join(vehicles, on="collision_id", how="left").join(
        persons, on="collision_id", how="left"
    )
    factor = F.coalesce(F.col("contributing_factor_vehicle_1"), F.lit("UNSPECIFIED"))

    return (
        joined.fillna(0, subset=NUMERIC_DEFAULTS)
        .withColumn("avg_vehicle_age", F.coalesce("avg_vehicle_age", F.lit(0.0)))
        .withColumn("avg_person_age", F.coalesce("avg_person_age", F.lit(0.0)))
        .withColumn("primary_factor", factor)
        .withColumn("primary_vehicle_type", F.coalesce("vehicle_type_code1", F.lit("UNKNOWN")))
        .withColumn("secondary_vehicle_type", F.coalesce("vehicle_type_code2", F.lit("UNKNOWN")))
        .withColumn("factor_driver_inattention", factor.rlike("INATTENTION|DISTRACTION").cast("int"))
        .withColumn("factor_speeding", factor.rlike("UNSAFE SPEED|SPEEDING").cast("int"))
        .withColumn("factor_yield", factor.rlike("FAILURE TO YIELD|TRAFFIC CONTROL").cast("int"))
        .withColumn("factor_impairment", factor.rlike("ALCOHOL|DRUGS|PRESCRIPTION").cast("int"))
        .withColumn("location_known", (F.col("latitude").isNotNull() & F.col("longitude").isNotNull()).cast("int"))
        .select(
            "collision_id",
            "year_month",
            "crash_date",
            "crash_month",
            "crash_hour",
            "day_of_week",
            "borough",
            "primary_factor",
            "primary_vehicle_type",
            "secondary_vehicle_type",
            "is_weekend",
            "is_night",
            "location_known",
            "persons_injured",
            "persons_killed",
            "pedestrians_injured",
            "cyclists_injured",
            "motorists_injured",
            "has_injury",
            "has_fatality",
            "vehicle_count",
            "passenger_car_count",
            "suv_wagon_count",
            "truck_van_count",
            "taxi_count",
            "bus_count",
            "two_wheeler_count",
            "avg_vehicle_age",
            "max_vehicle_occupants",
            "property_damage_vehicle_count",
            "driver_inattention_count",
            "unsafe_speed_count",
            "failure_to_yield_count",
            "impairment_count",
            "person_count",
            "occupant_count",
            "pedestrian_person_count",
            "cyclist_person_count",
            "minor_count",
            "senior_count",
            "avg_person_age",
            "female_person_count",
            "male_person_count",
            "safety_equipment_count",
            "factor_driver_inattention",
            "factor_speeding",
            "factor_yield",
            "factor_impairment",
        )
        .dropDuplicates(["collision_id"])
    )
