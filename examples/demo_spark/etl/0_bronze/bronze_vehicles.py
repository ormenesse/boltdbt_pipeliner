from __future__ import annotations

from pyspark.sql import functions as F


def _vehicle_type_group(col_name: str):
    vehicle = F.coalesce(F.col(col_name), F.lit("UNKNOWN"))
    return (
        F.when(vehicle.rlike("TAXI|LIVERY|FOR HIRE"), "taxi")
        .when(vehicle.rlike("BUS"), "bus")
        .when(vehicle.rlike("TRUCK|PICK-UP|VAN|TRACTOR|DUMP"), "truck_van")
        .when(vehicle.rlike("MOTORCYCLE|MOPED|SCOOTER|BIKE|BICYCLE"), "two_wheeler")
        .when(vehicle.rlike("SPORT UTILITY|SUV|STATION WAGON"), "suv_wagon")
        .when(vehicle.rlike("SEDAN|PASSENGER"), "passenger_car")
        .otherwise("other")
    )


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]
    current_year = F.year(F.current_date())
    sane_year = (F.col("vehicle_year") >= 1980) & (F.col("vehicle_year") <= current_year + 1)

    return (
        vehicles.fillna({"vehicle_occupants": 0})
        .withColumn("vehicle_type_group", _vehicle_type_group("vehicle_type"))
        .withColumn("vehicle_age", F.when(sane_year, current_year - F.col("vehicle_year")))
        .withColumn("driver_sex", F.coalesce("driver_sex", F.lit("UNKNOWN")))
        .withColumn("pre_crash", F.coalesce("pre_crash", F.lit("UNKNOWN")))
        .withColumn("point_of_impact", F.coalesce("point_of_impact", F.lit("UNKNOWN")))
        .withColumn("public_property_damage", F.coalesce("public_property_damage", F.lit("N")))
        .withColumn("contributing_factor_1", F.coalesce("contributing_factor_1", F.lit("UNSPECIFIED")))
        .withColumn("contributing_factor_2", F.coalesce("contributing_factor_2", F.lit("UNSPECIFIED")))
    )
