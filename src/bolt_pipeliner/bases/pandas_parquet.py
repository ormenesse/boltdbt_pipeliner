import datetime as dt
import json
import logging
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from bolt_pipeliner.bases._io import (
    detect_file_format,
    has_uri_scheme,
    resolve_data_path,
)

try:
    import fsspec
except ImportError:
    fsspec = None

logging.basicConfig(level=logging.INFO)


class ETLBaseParquetPandas:
    """
    Pandas/pyarrow ETL base:
      - No Spark or SQL.
      - Input: CSV, Parquet, Excel, or JSON (local paths or cloud URIs).
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
        self.input_tables: Dict[str, pd.DataFrame] = {}
        self.dataset_path = resolve_data_path(
            f"{self.layer}_{self.output_table_name}",
            bucket,
        )
        if self.layer == "flatfile":
            self.dataset_path = self.dataset_path.replace("flat_files", "data")

        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = tuple(partition_by) if partition_by else None
        self.df: Optional[pd.DataFrame] = None
        self.incremental = incremental
        self.year_months: Optional[list[int]] = None
        self.unload = unload
        self.storage_options = storage_options or {}
        self.incremental_column = incremental_column or self.DEFAULT_INCREMENTAL_COLUMN

    def _read_json_normalized(self, path: str, *, lines: bool) -> pd.DataFrame:
        if lines:
            records: list[dict[str, Any]] = []
            if has_uri_scheme(path) and fsspec is not None:
                opener = fsspec.open(path, mode="rt", **self.storage_options)
            else:
                opener = open(path, "r", encoding="utf-8")

            with opener as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))

            return pd.json_normalize(records)

        if has_uri_scheme(path) and fsspec is not None:
            opener = fsspec.open(path, mode="rt", **self.storage_options)
        else:
            opener = open(path, "r", encoding="utf-8")

        with opener as f:
            payload = json.load(f)

        return pd.json_normalize(payload)

    def _read_excel(self, path: str) -> pd.DataFrame:
        try:
            return pd.read_excel(path, storage_options=self.storage_options)
        except TypeError:
            return pd.read_excel(path)

    def _resolve_input_path(self, source: str) -> str:
        default_extension = ".parquet" if self.layer != "flatfile" else None
        return resolve_data_path(source, self.bucket, default_extension=default_extension)

    def _fs_exists(self, path: str) -> bool:
        if fsspec is None:
            try:
                pafs, pafspath = pa.fs.FileSystem.from_uri(path)
                return pafs.exists(pafspath)
            except Exception:
                return False
        fs, _, paths = fsspec.get_fs_token_paths(
            path, storage_options=self.storage_options
        )
        return fs.exists(paths[0])

    def _list_any_parquet_file(self, path: str) -> bool:
        try:
            dataset = ds.dataset(path, format="parquet", filesystem=None)
            _ = list(dataset.files)
            return True
        except Exception:
            return False

    def check_if_tables_exists_find_yearmonths(self):
        if not self.incremental:
            self.year_months = None
            return

        exists = self._fs_exists(self.dataset_path) and self._list_any_parquet_file(
            self.dataset_path
        )
        if not exists:
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

        for key, source in self.input_table_names.items():
            path = self._resolve_input_path(source)
            file_format = detect_file_format(path)

            if file_format == "csv":
                df = pd.read_csv(path, storage_options=self.storage_options)
            elif file_format == "parquet":
                df = pd.read_parquet(path, storage_options=self.storage_options)
            elif file_format == "excel":
                df = self._read_excel(path)
            elif file_format == "json":
                df = self._read_json_normalized(path, lines=False)
            elif file_format == "jsonl":
                df = self._read_json_normalized(path, lines=True)
            else:
                raise ValueError(
                    f"Unsupported input format for '{source}'. Supported flatfile formats: "
                    ".csv, .parquet, .xlsx/.xls, .json, .jsonl/.ndjson."
                )

            self.input_tables[key] = df
            logging.info(f"{self.logging_string} - Loaded - {key} from {path}")

    def unload_data(self, processed_df: pd.DataFrame):
        if processed_df is None or processed_df.empty:
            logging.info(f"{self.logging_string} - Nothing to write (empty df).")
            return

        df_to_write = processed_df.copy()

        if (
            self.year_months is not None
            and self.incremental_column in df_to_write.columns
        ):
            df_to_write = df_to_write[
                df_to_write[self.incremental_column].isin(self.year_months)
            ]

        logging.info(f"{self.logging_string} - Saving data to - {self.dataset_path} ...")

        table = pa.Table.from_pandas(df_to_write, preserve_index=False)

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

    def process_data(self, dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
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
