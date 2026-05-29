from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from bolt_pipeliner.bases._incremental import (
    build_incremental_policy,
    incremental_values_desc,
)


class LocalPolarsParquetBase:
    """Small local Polars base for examples that should run without S3."""

    DEFAULT_INCREMENTAL_COLUMN = "year_month"
    DEFAULT_INCREMENTAL_TYPE = "int"
    DEFAULT_INCREMENTAL_UNIT = 3
    DEFAULT_INCREMENTAL_DATE_GRAIN = "monthly"

    def __init__(
        self,
        layer,
        bucket,
        input_tables,
        output_table_name,
        partition_by=None,
        unload=True,
        incremental=False,
        incremental_column=None,
        incremental_type=None,
        incremental_unit=None,
        incremental_date_grain=None,
        **kwargs,
    ):
        self.layer = layer
        self.bucket = bucket
        self.input_table_names = input_tables or {}
        self.input_tables = {}
        self.output_table_name = output_table_name
        self.partition_by = tuple(partition_by) if partition_by else None
        self.unload = unload
        self.incremental = incremental
        self.incremental_policy = build_incremental_policy(
            enabled=incremental,
            column=incremental_column or self.DEFAULT_INCREMENTAL_COLUMN,
            unit=incremental_unit,
            value_type=incremental_type,
            date_grain=incremental_date_grain,
            default_window=self.DEFAULT_INCREMENTAL_UNIT,
            default_value_type=self.DEFAULT_INCREMENTAL_TYPE,
            default_date_grain=self.DEFAULT_INCREMENTAL_DATE_GRAIN,
        )
        self.incremental_column = self.incremental_policy.column
        self.df = None
        self.year_months = None  # Backward-compat alias.
        self.logging_string = f"{layer} {output_table_name}"
        self.input_root = Path(bucket)
        self.output_root = Path("data/layers") if layer == "flatfile" else Path(bucket)
        self.table_path = self.output_root / f"{layer}_{output_table_name}.parquet"

    def check_if_tables_exists_find_yearmonths(self):
        # Keep method name for API compatibility.
        self.year_months = None

    def _load_existing_dataset(self) -> pl.DataFrame:
        if not self.table_path.exists():
            return pl.DataFrame()

        try:
            dataset = ds.dataset(str(self.table_path), format="parquet")
            if not list(dataset.files):
                return pl.DataFrame()
        except Exception:
            return pl.DataFrame()

        return pl.read_parquet(str(self.table_path))

    def _normalize_incremental_polars(self, df: pl.DataFrame, *, frame_name: str) -> pl.DataFrame:
        marker = "__bp_incremental_value"
        if self.incremental_column not in df.columns:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' not found in {frame_name}."
            )

        if self.incremental_policy.value_type == "int":
            source = pl.col(self.incremental_column)
            normalized = source.cast(pl.Int64, strict=False)
            numeric = source.cast(pl.Float64, strict=False)
            invalid_expr = source.is_not_null() & (
                normalized.is_null() | (numeric != normalized.cast(pl.Float64))
            )
        else:
            normalized = pl.col(self.incremental_column).cast(pl.Date, strict=False)
            invalid_expr = (
                pl.col(self.incremental_column).is_not_null() & normalized.is_null()
            )

        out = df.with_columns(normalized.alias(marker))
        invalid = out.filter(invalid_expr)
        if invalid.height > 0:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' in {frame_name} has invalid "
                f"{self.incremental_policy.value_type} values."
            )

        if self.incremental_policy.value_type == "date":
            if self.incremental_policy.date_grain == "yearly":
                valid_grain = (pl.col(marker).dt.month() == 1) & (pl.col(marker).dt.day() == 1)
            elif self.incremental_policy.date_grain == "monthly":
                valid_grain = pl.col(marker).dt.day() == 1
            else:
                valid_grain = pl.lit(True)

            bad = out.filter(pl.col(marker).is_not_null() & (~valid_grain))
            if bad.height > 0:
                raise ValueError(
                    f"Incremental column '{self.incremental_column}' in {frame_name} must follow "
                    f"{self.incremental_policy.date_grain} date granularity."
                )

        return out

    def _apply_incremental_policy(self, incoming_df: pl.DataFrame) -> pl.DataFrame:
        if (not self.incremental_policy.enabled) or self.incremental_policy.mode == "overwrite":
            return incoming_df.clone()

        marker = "__bp_incremental_value"
        incoming = self._normalize_incremental_polars(
            incoming_df,
            frame_name="processed DataFrame",
        )

        existing = self._load_existing_dataset()
        if existing.is_empty():
            return incoming.drop(marker)

        existing = self._normalize_incremental_polars(
            existing,
            frame_name="existing target table",
        )

        existing_values = (
            existing.select(marker)
            .drop_nulls()
            .unique()
            .to_series()
            .to_list()
        )

        if self.incremental_policy.mode == "append":
            existing_set = set(existing_values)
            incoming_values = (
                incoming.select(marker)
                .drop_nulls()
                .unique()
                .to_series()
                .to_list()
            )
            values_to_append = [v for v in incoming_values if v not in existing_set]
            incoming_filtered = incoming.filter(pl.col(marker).is_in(values_to_append))
            return pl.concat(
                [existing.drop(marker), incoming_filtered.drop(marker)],
                how="diagonal_relaxed",
            )

        sorted_existing = incremental_values_desc(existing_values)
        latest_values = sorted_existing[: self.incremental_policy.window_size or 0]
        if not latest_values:
            return incoming.drop(marker)

        cutoff = latest_values[-1]
        incoming_recent = incoming.filter(pl.col(marker) >= pl.lit(cutoff))
        existing_retained = existing.filter(~pl.col(marker).is_in(latest_values))
        return pl.concat(
            [existing_retained.drop(marker), incoming_recent.drop(marker)],
            how="diagonal_relaxed",
        )

    def load_data(self, input_path=None):
        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                source = self.input_root / rel_path
                self.input_tables[key] = pl.read_csv(source)
            return

        for key, name in self.input_table_names.items():
            self.input_tables[key] = pl.read_parquet(self.output_root / f"{name}.parquet")

    def unload_data(self, processed_df):
        if processed_df is None or processed_df.is_empty():
            return

        df_to_write = self._apply_incremental_policy(processed_df)

        self.table_path.parent.mkdir(parents=True, exist_ok=True)
        table = df_to_write.to_arrow()
        try:
            pafs, pafspath = pa.fs.FileSystem.from_uri(str(self.table_path))
            if pafs.exists(pafspath):
                pafs.delete_dir(pafspath)
        except Exception:
            pass

        ds.write_dataset(
            data=table,
            base_dir=str(self.table_path),
            format="parquet",
            partitioning=self.partition_by,
            existing_data_behavior="overwrite_or_ignore",
        )

    def process_data(self, dfs):
        raise NotImplementedError("Override process_data in the job module.")

    def run(self):
        self.check_if_tables_exists_find_yearmonths()
        self.load_data(self.input_table_names)
        self.df = self.process_data(self.input_tables)
        if self.unload:
            self.unload_data(self.df)
