from __future__ import annotations

import polars as pl


def _clean_text(name: str) -> pl.Expr:
    value = pl.col(name).cast(pl.Utf8, strict=False).str.strip_chars()
    return pl.when(value.is_null() | (value == "")).then(None).otherwise(value.str.to_uppercase())


def process_data(self, input_tables):
    crashes = input_tables["crashes"]
    return (
        crashes.select(
            pl.col("collision_id").cast(pl.Int64, strict=False).alias("collision_id"),
            pl.col("crash_date").str.to_datetime(strict=False).dt.date().alias("crash_date"),
            pl.col("crash_time").cast(pl.Utf8, strict=False).alias("crash_time"),
            _clean_text("borough").alias("borough"),
            pl.col("zip_code").cast(pl.Utf8, strict=False).alias("zip_code"),
            pl.col("latitude").cast(pl.Float64, strict=False).alias("latitude"),
            pl.col("longitude").cast(pl.Float64, strict=False).alias("longitude"),
            pl.col("number_of_persons_injured").cast(pl.Int64, strict=False).alias("persons_injured"),
            pl.col("number_of_persons_killed").cast(pl.Int64, strict=False).alias("persons_killed"),
            pl.col("number_of_pedestrians_injured").cast(pl.Int64, strict=False).alias("pedestrians_injured"),
            pl.col("number_of_cyclist_injured").cast(pl.Int64, strict=False).alias("cyclists_injured"),
            pl.col("number_of_motorist_injured").cast(pl.Int64, strict=False).alias("motorists_injured"),
            _clean_text("contributing_factor_vehicle_1").alias("contributing_factor_vehicle_1"),
            _clean_text("contributing_factor_vehicle_2").alias("contributing_factor_vehicle_2"),
            _clean_text("vehicle_type_code1").alias("vehicle_type_code1"),
            _clean_text("vehicle_type_code2").alias("vehicle_type_code2"),
        )
        .with_columns(
            pl.col("crash_date").dt.strftime("%Y%m").cast(pl.Int64).alias("year_month"),
            pl.col("crash_time").str.split(":").list.get(0).cast(pl.Int64, strict=False).alias("crash_hour"),
        )
        .filter(pl.col("collision_id").is_not_null())
    )
