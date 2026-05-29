from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from bolt_pipeliner.bases._incremental import (
    apply_incremental_policy_pandas,
    build_incremental_policy,
)


class LocalPandasParquetBase:
    """Small local Pandas base for examples that should run without S3."""

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

    def _load_existing_dataset(self) -> pd.DataFrame:
        if not self.table_path.exists():
            return pd.DataFrame()

        try:
            has_parquet = any(self.table_path.rglob("*.parquet"))
        except Exception:
            has_parquet = False

        if not has_parquet:
            return pd.DataFrame()

        return pd.read_parquet(self.table_path)

    def load_data(self, input_path=None):
        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                source = self.input_root / rel_path
                self.input_tables[key] = pd.read_csv(source)
            return

        for key, name in self.input_table_names.items():
            self.input_tables[key] = pd.read_parquet(self.output_root / f"{name}.parquet")

    def unload_data(self, processed_df):
        if processed_df is None or processed_df.empty:
            return

        existing_df = self._load_existing_dataset()
        df_to_write = apply_incremental_policy_pandas(
            existing_df,
            processed_df,
            self.incremental_policy,
        )

        self.table_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df_to_write, preserve_index=False)
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
