from __future__ import annotations

from pathlib import Path


class LocalSparkParquetBase:
    """Small local Spark base for examples that should run without S3 or Iceberg."""

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
        self.table_path.parent.mkdir(parents=True, exist_ok=True)
        writer = processed_df.write.mode("overwrite")
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
