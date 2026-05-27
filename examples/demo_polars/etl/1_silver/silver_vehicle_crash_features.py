from __future__ import annotations

import polars as pl


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]
    factors = pl.concat_str(
        [pl.col("contributing_factor_1").fill_null(""), pl.col("contributing_factor_2").fill_null("")],
        separator=" ",
    )
    return vehicles.group_by("collision_id").agg(
        pl.len().cast(pl.Int64).alias("vehicle_count"),
        (pl.col("vehicle_type_group") == "passenger_car").sum().cast(pl.Int64).alias("passenger_car_count"),
        (pl.col("vehicle_type_group") == "suv_wagon").sum().cast(pl.Int64).alias("suv_wagon_count"),
        (pl.col("vehicle_type_group") == "truck_van").sum().cast(pl.Int64).alias("truck_van_count"),
        (pl.col("vehicle_type_group") == "taxi").sum().cast(pl.Int64).alias("taxi_count"),
        (pl.col("vehicle_type_group") == "bus").sum().cast(pl.Int64).alias("bus_count"),
        (pl.col("vehicle_type_group") == "two_wheeler").sum().cast(pl.Int64).alias("two_wheeler_count"),
        pl.col("vehicle_age").mean().alias("avg_vehicle_age"),
        pl.col("vehicle_occupants").max().cast(pl.Int64).alias("max_vehicle_occupants"),
        (pl.col("public_property_damage") == "Y").sum().cast(pl.Int64).alias("property_damage_vehicle_count"),
        factors.str.contains("INATTENTION|DISTRACTION").sum().cast(pl.Int64).alias("driver_inattention_count"),
        factors.str.contains("UNSAFE SPEED|SPEEDING").sum().cast(pl.Int64).alias("unsafe_speed_count"),
        factors.str.contains("FAILURE TO YIELD|TRAFFIC CONTROL").sum().cast(pl.Int64).alias("failure_to_yield_count"),
        factors.str.contains("ALCOHOL|DRUGS|PRESCRIPTION").sum().cast(pl.Int64).alias("impairment_count"),
    )
