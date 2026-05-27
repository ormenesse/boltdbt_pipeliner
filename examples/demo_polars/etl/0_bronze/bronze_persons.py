from __future__ import annotations

import polars as pl


def process_data(self, input_tables):
    persons = input_tables["persons"]
    return persons.with_columns(
        pl.when(pl.col("person_age").is_between(0, 100, closed="both"))
        .then(pl.col("person_age").cast(pl.Float64))
        .otherwise(None)
        .alias("person_age"),
        pl.col("person_type").fill_null("UNKNOWN"),
        pl.col("ped_role").fill_null("UNKNOWN"),
        pl.col("person_sex").fill_null("UNKNOWN"),
        pl.col("safety_equipment").fill_null("UNKNOWN"),
        pl.col("ejection").fill_null("UNKNOWN"),
    )
