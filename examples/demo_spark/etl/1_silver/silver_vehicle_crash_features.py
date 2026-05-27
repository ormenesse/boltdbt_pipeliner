from __future__ import annotations

from pyspark.sql import functions as F


def _count_when(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("int")


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]
    factors = F.concat_ws(" ", "contributing_factor_1", "contributing_factor_2")

    return vehicles.groupBy("collision_id").agg(
        F.count("*").cast("int").alias("vehicle_count"),
        _count_when(F.col("vehicle_type_group") == "passenger_car").alias("passenger_car_count"),
        _count_when(F.col("vehicle_type_group") == "suv_wagon").alias("suv_wagon_count"),
        _count_when(F.col("vehicle_type_group") == "truck_van").alias("truck_van_count"),
        _count_when(F.col("vehicle_type_group") == "taxi").alias("taxi_count"),
        _count_when(F.col("vehicle_type_group") == "bus").alias("bus_count"),
        _count_when(F.col("vehicle_type_group") == "two_wheeler").alias("two_wheeler_count"),
        F.avg("vehicle_age").alias("avg_vehicle_age"),
        F.max("vehicle_occupants").cast("int").alias("max_vehicle_occupants"),
        _count_when(F.col("public_property_damage") == "Y").alias("property_damage_vehicle_count"),
        _count_when(factors.rlike("INATTENTION|DISTRACTION")).alias("driver_inattention_count"),
        _count_when(factors.rlike("UNSAFE SPEED|SPEEDING")).alias("unsafe_speed_count"),
        _count_when(factors.rlike("FAILURE TO YIELD|TRAFFIC CONTROL")).alias("failure_to_yield_count"),
        _count_when(factors.rlike("ALCOHOL|DRUGS|PRESCRIPTION")).alias("impairment_count"),
    )
