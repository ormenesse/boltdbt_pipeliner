import logging

from pyspark.sql import functions as F

from bolt_pipeliner.bases._incremental import (
    build_incremental_policy,
    incremental_values_desc,
)
from bolt_pipeliner.bases._io import detect_file_format, resolve_data_path, to_pandas_path, to_spark_path

logging.basicConfig(level=logging.INFO)


class ETLBase:
    """
    ETL base for Spark + Iceberg (Glue).
    Rules:
      - flatfile: read CSV/Parquet/Excel/JSON from <bucket>/<path>
      - bronze: DO NOT CHANGE (SQL from self.catalog as in original)
      - else: read Iceberg from save_catalog.<fixed_schema>.<table>
      - writes: save_catalog.<fixed_schema>.<layer>_<output_table_name>
    """

    DEFAULT_FIXED_SCHEMA = "cxdw_dm"
    DEFAULT_INCREMENTAL_COLUMN = "year_month"
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
        catalog="shared_catalog",
        save_catalog="dev_catalog",
        fixed_schema=None,
        incremental_column=None,
        incremental_type=None,
        incremental_unit=None,
        incremental_date_grain=None,
        **kwargs,
    ):
        self.spark = spark
        self.layer = layer
        self.bucket = bucket
        self.input_table_names = input_tables or {}
        self.input_tables = {}
        self.catalog = catalog
        self.save_catalog = save_catalog
        self.partition_by = partition_by
        self.unload = unload
        self.incremental = incremental
        self.df = None
        self.year_months = None  # Backward-compat alias.
        self.output_table_name = output_table_name
        self.fixed_schema = fixed_schema or self.DEFAULT_FIXED_SCHEMA
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
        self.iceberg_table = f"{self.save_catalog}.{self.fixed_schema}.{self.layer}_{self.output_table_name}"
        self.logging_string = f"{self.layer} {self.output_table_name}"
        self.table_exists = self._table_exists(self.iceberg_table)

    def _read_excel(self, path: str):
        import pandas as pd

        excel_path = to_pandas_path(path)
        pdf = pd.read_excel(excel_path)
        return self.spark.createDataFrame(pdf)

    def _read_flatfile_source(self, source: str):
        path = to_spark_path(resolve_data_path(source, self.bucket))
        file_format = detect_file_format(path)

        if file_format == "csv":
            return self.spark.read.csv(
                path,
                header=True,
                inferSchema=True,
                multiLine=True,
                escape='"',
                quote='"',
            )
        if file_format == "parquet":
            return self.spark.read.parquet(path)
        if file_format == "json":
            return self.spark.read.option("multiLine", True).json(path)
        if file_format == "jsonl":
            return self.spark.read.json(path)
        if file_format == "excel":
            return self._read_excel(path)

        raise ValueError(
            f"Unsupported input format for '{source}'. Supported flatfile formats: "
            ".csv, .parquet, .xlsx/.xls, .json, .jsonl/.ndjson."
        )

    @property
    def FIXED_SCHEMA(self):
        """Back-compat alias; prefer `self.fixed_schema`."""
        return self.fixed_schema

    def _table_exists(self, table_ident: str) -> bool:
        try:
            return self.spark.catalog.tableExists(table_ident)
        except Exception:
            return False

    def _ensure_namespace(self, catalog: str, schema: str):
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")

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

        if not self._table_exists(self.iceberg_table):
            return incoming_df

        marker = "__bp_incremental_value"
        existing = self.spark.read.table(self.iceberg_table)
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

    def load_data(self, input_path=None):
        print(f"{self.logging_string} - Loading data...")

        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, source in self.input_table_names.items():
                self.input_tables[key] = self._read_flatfile_source(source)
                print(f"{self.logging_string} - Loaded flatfile - {source}")
            return

        if self.layer == "bronze":
            # DO NOT CHANGE: use the original SQL against self.catalog
            for key in self.input_table_names.keys():
                if "." in self.input_table_names[key]:
                    self.input_tables[key] = self.spark.sql(
                        f"""
                            SELECT *
                            FROM {self.catalog}.{self.input_table_names[key]}
                        """
                    )
                else:
                    table_ident = f"{self.save_catalog}.{self.fixed_schema}.{self.input_table_names[key]}"
                    self.input_tables[key] = self.spark.read.table(table_ident)
                print(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            return

        for key, name in self.input_table_names.items():
            table_ident = f"{self.save_catalog}.{self.fixed_schema}.{name}"
            self.input_tables[key] = self.spark.read.table(table_ident)
            print(f"{self.logging_string} - Loaded - {table_ident}")

    def _create_table(self, df):
        writer = df.writeTo(self.iceberg_table)
        if self.partition_by:
            writer = writer.partitionedBy(*[F.col(c) for c in self.partition_by])
        writer.createOrReplace()

    def _replace_table_partitions(self, df):
        df.writeTo(self.iceberg_table).overwritePartitions()

    def unload_data(self, processed_df):
        processed_df.cache()
        df_to_write = self._apply_incremental_policy(processed_df)
        print(f"{self.logging_string} - Saving data to Iceberg table - {self.iceberg_table}...")

        self._ensure_namespace(self.save_catalog, self.fixed_schema)

        self._create_table(df_to_write)
        self.table_exists = True

        print(f"{self.logging_string} - Data successfully saved to Iceberg - {self.iceberg_table}")

    def process_data(self, dfs):
        print(f"{self.logging_string} - Initializing processing...")
        raise NotImplementedError("Override this method in your job.")

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
