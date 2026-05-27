from __future__ import annotations

import polars as pl


CATEGORICAL_FEATURES = [
    "borough",
    "day_of_week",
    "primary_factor",
    "primary_vehicle_type",
    "secondary_vehicle_type",
]

NUMERIC_FEATURES = [
    "crash_month",
    "crash_hour",
    "is_weekend",
    "is_night",
    "location_known",
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


def process_data(self, input_tables):
    features = input_tables["features"]
    return (
        features.with_columns(
            pl.col("has_injury").cast(pl.Float64).fill_null(0.0).alias("label"),
            *[pl.col(col).cast(pl.Utf8).fill_null("UNKNOWN").alias(col) for col in CATEGORICAL_FEATURES],
            *[pl.col(col).cast(pl.Float64).fill_null(0.0).alias(col) for col in NUMERIC_FEATURES],
        )
        .select("collision_id", "year_month", "label", *CATEGORICAL_FEATURES, *NUMERIC_FEATURES)
        .unique(subset=["collision_id"], keep="first")
    )
