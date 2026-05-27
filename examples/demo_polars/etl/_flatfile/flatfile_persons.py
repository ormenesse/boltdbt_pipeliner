from __future__ import annotations

import polars as pl


def _clean_text(name: str) -> pl.Expr:
    value = pl.col(name).cast(pl.Utf8, strict=False).str.strip_chars()
    return pl.when(value.is_null() | (value == "")).then(None).otherwise(value.str.to_uppercase())


def process_data(self, input_tables):
    persons = input_tables["persons"]
    return (
        persons.select(
            pl.col("unique_id").cast(pl.Int64, strict=False).alias("person_record_id"),
            pl.col("collision_id").cast(pl.Int64, strict=False).alias("collision_id"),
            pl.col("person_id").cast(pl.Utf8, strict=False).alias("person_id"),
            _clean_text("person_type").alias("person_type"),
            pl.col("person_age").cast(pl.Int64, strict=False).alias("person_age"),
            _clean_text("ejection").alias("ejection"),
            _clean_text("safety_equipment").alias("safety_equipment"),
            _clean_text("ped_role").alias("ped_role"),
            _clean_text("contributing_factor_1").alias("contributing_factor_1"),
            _clean_text("contributing_factor_2").alias("contributing_factor_2"),
            _clean_text("person_sex").alias("person_sex"),
        )
        .filter(pl.col("collision_id").is_not_null())
        .unique(subset=["person_record_id"], keep="first")
    )
