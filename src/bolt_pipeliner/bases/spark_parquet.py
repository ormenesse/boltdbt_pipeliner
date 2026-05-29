import logging

from pyspark.sql import functions as F

from bolt_pipeliner.bases._incremental import (
    build_incremental_policy,
    incremental_values_desc,
)
from bolt_pipeliner.bases._io import detect_file_format, resolve_data_path, to_pandas_path, to_spark_path

logging.basicConfig(level=logging.INFO)


class ETLBaseParquet:
    DEFAULT_INCREMENTAL_COLUMN = "yearMonth"
    DEFAULT_INCREMENTAL_TYPE = "int"
    DEFAULT_INCREMENTAL_UNIT = 3
    DEFAULT_INCREMENTAL_DATE_GRAIN = "monthly"

    def __init__(
        self,
        spark,
        layer,
        bucket,
        input_tables,
        output_table_name,
        partition_by=None,
        unload=True,
        incremental=True,
        incremental_column=None,
        incremental_type=None,
        incremental_unit=None,
        incremental_date_grain=None,
        **kwargs,
    ):
        self.spark = spark
        self.layer = layer
        self.input_table_names = input_tables
        self.output_table_name = output_table_name
        self.bucket = bucket
        self.input_tables = {}
        self.parquet_path = to_spark_path(
            resolve_data_path(
                f"{self.layer}_{output_table_name}.parquet",
                bucket,
            )
        )
        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = partition_by
        self.df = None
        self.incremental = incremental
        self.year_months = None  # Backward-compat alias.
        self.unload = unload
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
        if self.layer == "flatfile":
            self.parquet_path = self.parquet_path.replace("flat_files", "data")

    def _read_excel(self, path: str):
        import pandas as pd

        excel_path = to_pandas_path(path)
        pdf = pd.read_excel(excel_path)
        return self.spark.createDataFrame(pdf)

    def _read_input_df(self, source: str):
        resolved_path = to_spark_path(resolve_data_path(source, self.bucket))
        file_format = detect_file_format(resolved_path)

        if file_format == "csv":
            return (
                self.spark.read.option("header", True)
                .option("inferSchema", True)
                .option("multiLine", True)
                .option("escape", '"')
                .option("quote", '"')
                .csv(resolved_path)
            )
        if file_format == "parquet":
            return self.spark.read.parquet(resolved_path)
        if file_format == "json":
            return self.spark.read.option("multiLine", True).json(resolved_path)
        if file_format == "jsonl":
            return self.spark.read.json(resolved_path)
        if file_format == "excel":
            return self._read_excel(resolved_path)

        raise ValueError(
            f"Unsupported input format for '{source}'. Supported flatfile formats: "
            ".csv, .parquet, .xlsx/.xls, .json, .jsonl/.ndjson."
        )

    def _target_exists(self) -> bool:
        try:
            self.spark.read.parquet(self.parquet_path).limit(1).collect()
            return True
        except Exception:
            return False

    def check_if_tables_exists_find_yearmonths(self):
        # Keep method name for API compatibility.
        self.year_months = None

    def _normalize_incremental_df(self, df, *, frame_name: str):
        marker = "__bp_incremental_value"
        if self.incremental_column not in df.columns:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' not found in {frame_name}."
            )

        if self.incremental_policy.value_type == "int":
            source = F.col(self.incremental_column)
            normalized = source.cast("long")
            numeric = source.cast("double")
            invalid_condition = source.isNotNull() & (
                normalized.isNull() | (numeric != normalized.cast("double"))
            )
        else:
            normalized = F.to_date(F.col(self.incremental_column))
            invalid_condition = (
                F.col(self.incremental_column).isNotNull() & normalized.isNull()
            )

        out = df.withColumn(marker, normalized)
        invalid = out.filter(invalid_condition).limit(1)
        if invalid.count() > 0:
            raise ValueError(
                f"Incremental column '{self.incremental_column}' in {frame_name} has invalid "
                f"{self.incremental_policy.value_type} values."
            )

        if self.incremental_policy.value_type == "date":
            if self.incremental_policy.date_grain == "yearly":
                valid_grain = (
                    (F.month(F.col(marker)) == 1)
                    & (F.dayofmonth(F.col(marker)) == 1)
                )
            elif self.incremental_policy.date_grain == "monthly":
                valid_grain = F.dayofmonth(F.col(marker)) == 1
            else:
                valid_grain = F.lit(True)

            bad = out.filter(F.col(marker).isNotNull() & (~valid_grain)).limit(1)
            if bad.count() > 0:
                raise ValueError(
                    f"Incremental column '{self.incremental_column}' in {frame_name} must follow "
                    f"{self.incremental_policy.date_grain} date granularity."
                )

        return out

    def _apply_incremental_policy(self, incoming_df):
        if (not self.incremental_policy.enabled) or self.incremental_policy.mode == "overwrite":
            return incoming_df

        if not self._target_exists():
            return incoming_df

        marker = "__bp_incremental_value"
        existing = self.spark.read.parquet(self.parquet_path)
        incoming_norm = self._normalize_incremental_df(incoming_df, frame_name="processed DataFrame")
        existing_norm = self._normalize_incremental_df(existing, frame_name="existing target table")

        existing_values = [
            row[0]
            for row in existing_norm.select(marker).where(F.col(marker).isNotNull()).distinct().collect()
        ]

        if self.incremental_policy.mode == "append":
            existing_values_df = existing_norm.select(marker).where(
                F.col(marker).isNotNull()
            ).distinct()
            incoming_filtered = incoming_norm.join(existing_values_df, marker, "left_anti")
            return existing.unionByName(incoming_filtered.drop(marker), allowMissingColumns=True)

        sorted_existing = incremental_values_desc(existing_values)
        latest_values = sorted_existing[: self.incremental_policy.window_size or 0]
        if not latest_values:
            return incoming_df

        cutoff = latest_values[-1]
        incoming_recent = incoming_norm.filter(F.col(marker) >= F.lit(cutoff)).drop(marker)
        existing_retained = existing_norm.filter(~F.col(marker).isin(latest_values)).drop(marker)
        return existing_retained.unionByName(incoming_recent, allowMissingColumns=True)

    def load_data(self, input_path):
        logging.info(f"{self.logging_string} - Loading data...")
        if self.input_table_names is None:
            return

        if self.layer == "bronze":
            for key in self.input_table_names.keys():
                self.input_tables[key] = self.spark.sql(
                    f"""
                        SELECT *
                        FROM shared_catalog.{self.input_table_names[key]}
                    """
                )
                logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
        elif self.layer == "flatfile":
            for key, source in self.input_table_names.items():
                self.input_tables[key] = self._read_input_df(source)
                logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
        else:
            for key, source in self.input_table_names.items():
                source_path = resolve_data_path(source, self.bucket, default_extension=".parquet")
                self.input_tables[key] = self._read_input_df(source_path)
                logging.info(f"{self.logging_string} - Loaded - {source}")

    def unload_data(self, processed_df):
        processed_df.cache()
        df_to_write = self._apply_incremental_policy(processed_df)

        logging.info(
            f"{self.logging_string} - Saving data to - {self.parquet_path} "
            f"(incremental_mode={self.incremental_policy.mode})..."
        )
        if self.partition_by is None:
            df_to_write.write.mode("overwrite").parquet(self.parquet_path)
        else:
            df_to_write.write.partitionBy(*self.partition_by).mode("overwrite").parquet(
                self.parquet_path
            )
        logging.info(f"{self.logging_string} - Data successfully saved to - {self.parquet_path}")

    def process_data(self, dfs):
        logging.info(f"{self.logging_string} - Initializing processing...")
        raise NotImplementedError("This method should be overridden by subclasses.")

    def run(self):
        self.check_if_tables_exists_find_yearmonths()
        self.load_data(self.input_table_names)

        if hasattr(self, "process_data"):
            processed_df = self.process_data(self.input_tables)
            self.df = processed_df
        else:
            raise NotImplementedError("No process_data method defined.")

        if self.unload:
            self.unload_data(self.df)
