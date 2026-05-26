import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

logging.basicConfig(level=logging.INFO)


class ETLBaseParquetPolars:
    """
    Polars/pyarrow ETL base:
      - Input: CSV or Parquet (local or S3 paths).
      - Output: Parquet (optionally partitioned).
      - Incremental: process only the last 3 months if an existing output dataset already exists.
    """

    DEFAULT_INCREMENTAL_COLUMN = "yearMonth"

    def __init__(
        self,
        layer: str,
        bucket: Optional[str],
        input_tables: Dict[str, str],
        output_table_name: str,
        partition_by: Optional[Iterable[str]] = None,
        unload: bool = True,
        incremental: bool = True,
        storage_options: Optional[Dict[str, Any]] = None,
        incremental_column: Optional[str] = None,
        **kwargs,
    ):
        self.layer = layer
        self.input_table_names = input_tables
        self.output_table_name = output_table_name
        self.bucket = bucket
        self.input_tables: Dict[str, pl.DataFrame] = {}
        base = f"{bucket}/" if bucket else ""
        self.dataset_path = f"{base}{self.layer}_{self.output_table_name}"
        if self.layer == "flatfile":
            self.dataset_path = self.dataset_path.replace("flat_files", "data")

        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = tuple(partition_by) if partition_by else None
        self.df: Optional[pl.DataFrame] = None
        self.incremental = incremental
        self.year_months: Optional[list[int]] = None
        self.unload = unload
        self.storage_options = storage_options or {}
        self.extra_args = kwargs
        self.incremental_column = incremental_column or self.DEFAULT_INCREMENTAL_COLUMN

    def check_if_tables_exists_find_yearmonths(self):
        if not self.incremental:
            self.year_months = None
            return

        try:
            dataset = ds.dataset(self.dataset_path, format="parquet")
            _ = list(dataset.files)
        except Exception:
            self.year_months = None
            return

        today = dt.date.today()
        start = today.replace(day=1) - dt.timedelta(days=31 * 3)
        month_seq = [(start.year * 100 + start.month)]
        while month_seq[-1] < (today.year * 100 + today.month):
            y, m = divmod(month_seq[-1], 100)
            if m == 12:
                y += 1
                m = 1
            else:
                m += 1
            month_seq.append(y * 100 + m)
        self.year_months = month_seq

    def load_data(self):
        logging.info(f"{self.logging_string} - Loading data...")
        if not self.input_table_names:
            return

        for key, path in self.input_table_names.items():
            if path.lower().endswith(".csv"):
                df = pl.read_csv(path, **self.storage_options)
            else:
                df = pl.read_parquet(path, **self.storage_options)
            self.input_tables[key] = df
            logging.info(f"{self.logging_string} - Loaded - {key} from {path}")

    def unload_data(self, processed_df: pl.DataFrame):
        if processed_df is None or processed_df.is_empty():
            logging.info(f"{self.logging_string} - Nothing to write (empty df).")
            return

        df_to_write = processed_df.clone()

        if (
            self.year_months is not None
            and self.incremental_column in df_to_write.columns
        ):
            df_to_write = df_to_write.filter(
                pl.col(self.incremental_column).is_in(self.year_months)
            )

        logging.info(f"{self.logging_string} - Saving data to - {self.dataset_path}...")

        table = df_to_write.to_arrow()

        try:
            pafs, pafspath = pa.fs.FileSystem.from_uri(self.dataset_path)
            if pafs.exists(pafspath):
                pafs.delete_dir(pafspath)
        except Exception:
            pass

        ds.write_dataset(
            data=table,
            base_dir=self.dataset_path,
            format="parquet",
            partitioning=self.partition_by,
            existing_data_behavior="overwrite_or_ignore",
        )

        logging.info(f"{self.logging_string} - Data successfully saved to - {self.dataset_path}")

    def process_data(self, dfs: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        raise NotImplementedError("This method should be overridden by subclasses.")

    def run(self):
        self.check_if_tables_exists_find_yearmonths()
        self.load_data()
        if not hasattr(self, "process_data"):
            raise NotImplementedError("No process_data method defined.")
        processed_df = self.process_data(self.input_tables)
        self.df = processed_df
        if self.unload:
            self.unload_data(self.df)
