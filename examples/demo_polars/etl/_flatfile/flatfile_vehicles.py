from __future__ import annotations

import polars as pl


def _clean_text(name: str) -> pl.Expr:
    value = pl.col(name).cast(pl.Utf8, strict=False).str.strip_chars()
    return pl.when(value.is_null() | (value == "")).then(None).otherwise(value.str.to_uppercase())


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"]
    return (
        vehicles.select(
            pl.col("unique_id").cast(pl.Int64, strict=False).alias("vehicle_record_id"),
            pl.col("collision_id").cast(pl.Int64, strict=False).alias("collision_id"),
            pl.col("vehicle_id").cast(pl.Utf8, strict=False).alias("vehicle_id"),
            _clean_text("vehicle_type").alias("vehicle_type"),
            pl.col("vehicle_year").cast(pl.Int64, strict=False).alias("vehicle_year"),
            pl.col("vehicle_occupants").cast(pl.Int64, strict=False).alias("vehicle_occupants"),
            _clean_text("driver_sex").alias("driver_sex"),
            _clean_text("pre_crash").alias("pre_crash"),
            _clean_text("point_of_impact").alias("point_of_impact"),
            _clean_text("public_property_damage").alias("public_property_damage"),
            _clean_text("contributing_factor_1").alias("contributing_factor_1"),
            _clean_text("contributing_factor_2").alias("contributing_factor_2"),
        )
        .filter(pl.col("collision_id").is_not_null())
        .unique(subset=["vehicle_record_id"], keep="first")
    )
