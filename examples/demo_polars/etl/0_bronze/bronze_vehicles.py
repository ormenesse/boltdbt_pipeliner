from __future__ import annotations

from datetime import datetime

import polars as pl


def _vehicle_type_group() -> pl.Expr:
    vehicle = pl.col("vehicle_type").fill_null("UNKNOWN")
    return (
        pl.when(vehicle.str.contains("TAXI|LIVERY|FOR HIRE"))
        .then(pl.lit("taxi"))
        .when(vehicle.str.contains("BUS"))
        .then(pl.lit("bus"))
        .when(vehicle.str.contains("TRUCK|PICK-UP|VAN|TRACTOR|DUMP"))
        .then(pl.lit("truck_van"))
        .when(vehicle.str.contains("MOTORCYCLE|MOPED|SCOOTER|BIKE|BICYCLE"))
        .then(pl.lit("two_wheeler"))
        .when(vehicle.str.contains("SPORT UTILITY|SUV|STATION WAGON"))
        .then(pl.lit("suv_wagon"))
        .when(vehicle.str.contains("SEDAN|PASSENGER"))
        .then(pl.lit("passenger_car"))
        .otherwise(pl.lit("other"))
    )


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]
    current_year = datetime.now().year
    return vehicles.with_columns(
        pl.col("vehicle_occupants").fill_null(0).cast(pl.Int64),
        _vehicle_type_group().alias("vehicle_type_group"),
        pl.when(
            pl.col("vehicle_year").is_between(1980, current_year + 1, closed="both")
        )
        .then((current_year - pl.col("vehicle_year")).cast(pl.Float64))
        .otherwise(None)
        .alias("vehicle_age"),
        pl.col("driver_sex").fill_null("UNKNOWN"),
        pl.col("pre_crash").fill_null("UNKNOWN"),
        pl.col("point_of_impact").fill_null("UNKNOWN"),
        pl.col("public_property_damage").fill_null("N"),
        pl.col("contributing_factor_1").fill_null("UNSPECIFIED"),
        pl.col("contributing_factor_2").fill_null("UNSPECIFIED"),
    )
