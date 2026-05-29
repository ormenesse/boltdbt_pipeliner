from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from bolt_pipeliner.bases._incremental import (
    build_incremental_policy,
    incremental_values_desc,
)


class LocalSparkParquetBase:
    """Small local Spark base for examples that should run without S3 or Iceberg."""

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
        spark=None,
        partition_by=None,
        unload=True,
        incremental=False,
        incremental_column=None,
        incremental_type=None,
        incremental_unit=None,
        incremental_date_grain=None,
        **kwargs,
    ):
        if spark is None:
            from bolt_pipeliner.sessions import create_session

            spark = create_session("local")
        self.spark = spark
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

    def _target_exists(self) -> bool:
        try:
            self.spark.read.parquet(str(self.table_path)).limit(1).collect()
            return True
        except Exception:
            return False

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
                    (F.month(F.col(marker)) == 1) & (F.dayofmonth(F.col(marker)) == 1)
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
        existing = self.spark.read.parquet(str(self.table_path))
        incoming_norm = self._normalize_incremental_df(
            incoming_df,
            frame_name="processed DataFrame",
        )
        existing_norm = self._normalize_incremental_df(
            existing,
            frame_name="existing target table",
        )

        existing_values = [
            row[0]
            for row in existing_norm.select(marker)
            .where(F.col(marker).isNotNull())
            .distinct()
            .collect()
        ]

        if self.incremental_policy.mode == "append":
            existing_values_df = existing_norm.select(marker).where(
                F.col(marker).isNotNull()
            ).distinct()
            incoming_filtered = incoming_norm.join(existing_values_df, marker, "left_anti")
            return existing.unionByName(
                incoming_filtered.drop(marker),
                allowMissingColumns=True,
            )

        sorted_existing = incremental_values_desc(existing_values)
        latest_values = sorted_existing[: self.incremental_policy.window_size or 0]
        if not latest_values:
            return incoming_df

        cutoff = latest_values[-1]
        incoming_recent = incoming_norm.filter(F.col(marker) >= F.lit(cutoff)).drop(marker)
        existing_retained = existing_norm.filter(~F.col(marker).isin(latest_values)).drop(marker)
        return existing_retained.unionByName(incoming_recent, allowMissingColumns=True)

    def load_data(self, input_path=None):
        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                source = self.input_root / rel_path
                self.input_tables[key] = (
                    self.spark.read.option("header", True)
                    .option("inferSchema", True)
                    .option("multiLine", True)
                    .option("escape", '"')
                    .option("quote", '"')
                    .csv(str(source))
                )
            return

        for key, name in self.input_table_names.items():
            self.input_tables[key] = self.spark.read.parquet(
                str(self.output_root / f"{name}.parquet")
            )

    def unload_data(self, processed_df):
        if processed_df is None:
            return

        df_to_write = self._apply_incremental_policy(processed_df)
        self.table_path.parent.mkdir(parents=True, exist_ok=True)
        writer = df_to_write.write.mode("overwrite")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(str(self.table_path))

    def process_data(self, dfs):
        raise NotImplementedError("Override process_data in the job module.")

    def run(self):
        self.check_if_tables_exists_find_yearmonths()
        self.load_data(self.input_table_names)
        self.df = self.process_data(self.input_tables)
        if self.unload:
            self.unload_data(self.df)
