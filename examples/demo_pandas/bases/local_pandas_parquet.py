from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


class LocalPandasParquetBase:
    """Small local Pandas base for examples that should run without S3."""

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
        **kwargs,
    ):
        self.layer = layer
        self.bucket = bucket
        self.input_table_names = input_tables or {}
        self.input_tables = {}
        self.output_table_name = output_table_name
        self.partition_by = partition_by or []
        self.unload = unload
        self.incremental = incremental
        self.incremental_column = incremental_column or "year_month"
        self.df = None
        self.year_months = None
        self.logging_string = f"{layer} {output_table_name}"
        self.input_root = Path(bucket)
        self.output_root = Path("data/layers") if layer == "flatfile" else Path(bucket)
        self.table_path = self.output_root / f"{layer}_{output_table_name}.parquet"

    def check_if_tables_exists_find_yearmonths(self):
        self.year_months = None

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
        self.table_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(processed_df, preserve_index=False)
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
