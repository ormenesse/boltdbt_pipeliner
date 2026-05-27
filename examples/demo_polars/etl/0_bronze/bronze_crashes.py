from __future__ import annotations

import polars as pl


def process_data(self, input_tables):
    crashes = input_tables["crashes"]
    return (
        crashes.with_columns(
            pl.col("persons_injured").fill_null(0).cast(pl.Int64),
            pl.col("persons_killed").fill_null(0).cast(pl.Int64),
            pl.col("pedestrians_injured").fill_null(0).cast(pl.Int64),
            pl.col("cyclists_injured").fill_null(0).cast(pl.Int64),
            pl.col("motorists_injured").fill_null(0).cast(pl.Int64),
            pl.col("crash_hour").fill_null(0).cast(pl.Int64),
            pl.col("borough").fill_null("UNKNOWN"),
            pl.col("contributing_factor_vehicle_1").fill_null("UNSPECIFIED"),
            pl.col("contributing_factor_vehicle_2").fill_null("UNSPECIFIED"),
            pl.col("vehicle_type_code1").fill_null("UNKNOWN"),
            pl.col("vehicle_type_code2").fill_null("UNKNOWN"),
        )
        .with_columns(
            pl.col("crash_date").dt.month().alias("crash_month"),
            pl.col("crash_date").dt.strftime("%a").alias("day_of_week"),
            pl.col("crash_date").dt.weekday().is_in([6, 7]).cast(pl.Int64).alias("is_weekend"),
            ((pl.col("crash_hour") >= 22) | (pl.col("crash_hour") <= 5)).cast(pl.Int64).alias("is_night"),
            (pl.col("persons_injured") > 0).cast(pl.Int64).alias("has_injury"),
            (pl.col("persons_killed") > 0).cast(pl.Int64).alias("has_fatality"),
        )
        .unique(subset=["collision_id"], keep="first")
    )
