from __future__ import annotations

import polars as pl


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
    factor = pl.col("contributing_factor_vehicle_1").fill_null("UNSPECIFIED")

    columns = [
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
    ]

    return (
        joined.with_columns(
            *[pl.col(col).fill_null(0).cast(pl.Int64).alias(col) for col in NUMERIC_DEFAULTS],
            pl.col("avg_vehicle_age").fill_null(0.0).cast(pl.Float64).alias("avg_vehicle_age"),
            pl.col("avg_person_age").fill_null(0.0).cast(pl.Float64).alias("avg_person_age"),
            factor.alias("primary_factor"),
            pl.col("vehicle_type_code1").fill_null("UNKNOWN").alias("primary_vehicle_type"),
            pl.col("vehicle_type_code2").fill_null("UNKNOWN").alias("secondary_vehicle_type"),
            factor.str.contains("INATTENTION|DISTRACTION").cast(pl.Int64).alias("factor_driver_inattention"),
            factor.str.contains("UNSAFE SPEED|SPEEDING").cast(pl.Int64).alias("factor_speeding"),
            factor.str.contains("FAILURE TO YIELD|TRAFFIC CONTROL").cast(pl.Int64).alias("factor_yield"),
            factor.str.contains("ALCOHOL|DRUGS|PRESCRIPTION").cast(pl.Int64).alias("factor_impairment"),
            (pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())
            .cast(pl.Int64)
            .alias("location_known"),
        )
        .select(columns)
        .unique(subset=["collision_id"], keep="first")
    )
