import json
import logging
from typing import Any, Dict, Iterable, Optional

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

try:
    import fsspec
except ImportError:
    fsspec = None

from bolt_pipeliner.bases._incremental import (
    build_incremental_policy,
    incremental_values_desc,
)
from bolt_pipeliner.bases._io import (
    detect_file_format,
    has_uri_scheme,
    resolve_data_path,
)

logging.basicConfig(level=logging.INFO)


class ETLBaseParquetPolars:
    """
    Polars/pyarrow ETL base:
      - Input: CSV, Parquet, Excel, or JSON (local paths or cloud URIs).
      - Output: Parquet (optionally partitioned).
      - Incremental modes:
          * window: rewrite last N values from incremental_column (+ new values)
          * append: write only values not already present
          * overwrite: rewrite the full table
    """

    DEFAULT_INCREMENTAL_COLUMN = "yearMonth"
    DEFAULT_INCREMENTAL_TYPE = "int"
    DEFAULT_INCREMENTAL_UNIT = 3
    DEFAULT_INCREMENTAL_DATE_GRAIN = "monthly"

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
        incremental_type: Optional[str] = None,
        incremental_unit: Optional[int | str] = None,
        incremental_date_grain: Optional[str] = None,
        **kwargs,
    ):
        self.layer = layer
        self.input_table_names = input_tables
        self.output_table_name = output_table_name
        self.bucket = bucket
        self.input_tables: Dict[str, pl.DataFrame] = {}
        self.dataset_path = resolve_data_path(
            f"{self.layer}_{self.output_table_name}",
            bucket,
        )
        if self.layer == "flatfile":
            self.dataset_path = self.dataset_path.replace("flat_files", "data")

        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = tuple(partition_by) if partition_by else None
        self.df: Optional[pl.DataFrame] = None
        self.incremental = incremental
        self.year_months: Optional[list[int]] = None  # Backward-compat alias.
        self.unload = unload
        self.storage_options = storage_options or {}
        self.extra_args = kwargs
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

    def _resolve_input_path(self, source: str) -> str:
        default_extension = ".parquet" if self.layer != "flatfile" else None
        return resolve_data_path(source, self.bucket, default_extension=default_extension)

    def _read_excel(self, path: str) -> pl.DataFrame:
        if hasattr(pl, "read_excel"):
            try:
                return pl.read_excel(path)
            except Exception:
                pass

        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - exercised when pandas missing.
            raise RuntimeError(
                "Excel input requires either polars.read_excel support or pandas installed."
            ) from exc

        return pl.from_pandas(pd.read_excel(path))

    def _read_json_normalized(self, path: str, *, lines: bool) -> pl.DataFrame:
        try:
            import pandas as pd
        except ImportError:
            if lines:
                return pl.read_ndjson(path, **self.storage_options)
            return pl.read_json(path, **self.storage_options)

        if has_uri_scheme(path) and fsspec is not None:
            opener = fsspec.open(path, mode="rt", **self.storage_options)
        else:
            opener = open(path, "r", encoding="utf-8")

        with opener as f:
            if lines:
                payload = [json.loads(line) for line in f if line.strip()]
            else:
                payload = json.load(f)

        return pl.from_pandas(pd.json_normalize(payload))

    def check_if_tables_exists_find_yearmonths(self):
        # Keep method name for API compatibility.
        self.year_months = None

    def _load_existing_dataset(self) -> pl.DataFrame:
        try:
            dataset = ds.dataset(self.dataset_path, format="parquet")
            if not list(dataset.files):
                return pl.DataFrame()
        except Exception:
            return pl.DataFrame()

        try:
            return pl.read_parquet(self.dataset_path, **self.storage_options)
        except Exception:
            table = ds.dataset(self.dataset_path, format="parquet").to_table()
            return pl.from_arrow(table)

    def _normalize_incremental_polars(self, df: pl.DataFrame, *, frame_name: str) -> pl.DataFrame:
        marker = "__bp_incremental_value"
        if self.incremental_column not in df.columns:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' not found in {frame_name}."
            )

        if self.incremental_policy.value_type == "int":
            normalized = pl.col(self.incremental_column).cast(pl.Int64, strict=False)
        else:
            normalized = pl.col(self.incremental_column).cast(pl.Date, strict=False)

        out = df.with_columns(normalized.alias(marker))
        invalid = out.filter(
            pl.col(self.incremental_column).is_not_null()
            & pl.col(marker).is_null()
        )
        if invalid.height > 0:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' in {frame_name} has invalid "
                f"{self.incremental_policy.value_type} values."
            )

        if self.incremental_policy.value_type == "date":
            if self.incremental_policy.date_grain == "yearly":
                valid_grain = (
                    (pl.col(marker).dt.month() == 1)
                    & (pl.col(marker).dt.day() == 1)
                )
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

    def load_data(self):
        logging.info(f"{self.logging_string} - Loading data...")
        if not self.input_table_names:
            return

        for key, source in self.input_table_names.items():
            path = self._resolve_input_path(source)
            file_format = detect_file_format(path)

            if file_format == "csv":
                df = pl.read_csv(path, **self.storage_options)
            elif file_format == "parquet":
                df = pl.read_parquet(path, **self.storage_options)
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

    def unload_data(self, processed_df: pl.DataFrame):
        if processed_df is None or processed_df.is_empty():
            logging.info(f"{self.logging_string} - Nothing to write (empty df).")
            return

        df_to_write = self._apply_incremental_policy(processed_df)

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
