from __future__ import annotations

import pandas as pd


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"].copy()
    factors = (
        vehicles["contributing_factor_1"].fillna("") + " " + vehicles["contributing_factor_2"].fillna("")
    )

    work = vehicles.assign(
        passenger_car_count=(vehicles["vehicle_type_group"] == "passenger_car").astype(int),
        suv_wagon_count=(vehicles["vehicle_type_group"] == "suv_wagon").astype(int),
        truck_van_count=(vehicles["vehicle_type_group"] == "truck_van").astype(int),
        taxi_count=(vehicles["vehicle_type_group"] == "taxi").astype(int),
        bus_count=(vehicles["vehicle_type_group"] == "bus").astype(int),
        two_wheeler_count=(vehicles["vehicle_type_group"] == "two_wheeler").astype(int),
        property_damage_vehicle_count=(vehicles["public_property_damage"] == "Y").astype(int),
        driver_inattention_count=factors.str.contains("INATTENTION|DISTRACTION", na=False).astype(int),
        unsafe_speed_count=factors.str.contains("UNSAFE SPEED|SPEEDING", na=False).astype(int),
        failure_to_yield_count=factors.str.contains("FAILURE TO YIELD|TRAFFIC CONTROL", na=False).astype(int),
        impairment_count=factors.str.contains("ALCOHOL|DRUGS|PRESCRIPTION", na=False).astype(int),
    )

    grouped = (
        work.groupby("collision_id", as_index=False)
        .agg(
            vehicle_count=("collision_id", "size"),
            passenger_car_count=("passenger_car_count", "sum"),
            suv_wagon_count=("suv_wagon_count", "sum"),
            truck_van_count=("truck_van_count", "sum"),
            taxi_count=("taxi_count", "sum"),
            bus_count=("bus_count", "sum"),
            two_wheeler_count=("two_wheeler_count", "sum"),
            avg_vehicle_age=("vehicle_age", "mean"),
            max_vehicle_occupants=("vehicle_occupants", "max"),
            property_damage_vehicle_count=("property_damage_vehicle_count", "sum"),
            driver_inattention_count=("driver_inattention_count", "sum"),
            unsafe_speed_count=("unsafe_speed_count", "sum"),
            failure_to_yield_count=("failure_to_yield_count", "sum"),
            impairment_count=("impairment_count", "sum"),
        )
    )

    int_cols = [
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
    ]
    grouped[int_cols] = grouped[int_cols].fillna(0).astype("Int64")
    grouped["avg_vehicle_age"] = grouped["avg_vehicle_age"].fillna(0.0)
    return grouped
