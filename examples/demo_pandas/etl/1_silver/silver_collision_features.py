from __future__ import annotations

import pandas as pd


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

    joined = crashes.merge(vehicles, on="collision_id", how="left").merge(
        persons, on="collision_id", how="left"
    )

    joined[NUMERIC_DEFAULTS] = joined[NUMERIC_DEFAULTS].apply(pd.to_numeric, errors="coerce").fillna(0)
    joined["avg_vehicle_age"] = pd.to_numeric(joined["avg_vehicle_age"], errors="coerce").fillna(0.0)
    joined["avg_person_age"] = pd.to_numeric(joined["avg_person_age"], errors="coerce").fillna(0.0)

    factor = joined["contributing_factor_vehicle_1"].fillna("UNSPECIFIED")
    joined["primary_factor"] = factor
    joined["primary_vehicle_type"] = joined["vehicle_type_code1"].fillna("UNKNOWN")
    joined["secondary_vehicle_type"] = joined["vehicle_type_code2"].fillna("UNKNOWN")
    joined["factor_driver_inattention"] = factor.str.contains("INATTENTION|DISTRACTION", na=False).astype("Int64")
    joined["factor_speeding"] = factor.str.contains("UNSAFE SPEED|SPEEDING", na=False).astype("Int64")
    joined["factor_yield"] = factor.str.contains("FAILURE TO YIELD|TRAFFIC CONTROL", na=False).astype("Int64")
    joined["factor_impairment"] = factor.str.contains("ALCOHOL|DRUGS|PRESCRIPTION", na=False).astype("Int64")
    joined["location_known"] = (joined["latitude"].notna() & joined["longitude"].notna()).astype("Int64")

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

    return joined[columns].drop_duplicates(subset=["collision_id"])
