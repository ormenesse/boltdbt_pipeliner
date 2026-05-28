import datetime as dt
import logging

from pyspark.sql import functions as F

from bolt_pipeliner.bases._io import detect_file_format, resolve_data_path, to_pandas_path, to_spark_path

logging.basicConfig(level=logging.INFO)


class ETLBaseParquet:
    DEFAULT_INCREMENTAL_COLUMN = "yearMonth"

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
        self.year_months = None
        self.unload = unload
        self.incremental_column = incremental_column or self.DEFAULT_INCREMENTAL_COLUMN
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

    def check_if_tables_exists_find_yearmonths(self):
        if self.incremental:
            try:
                schema = self.spark.read.parquet(self.parquet_path).printSchema()
                del schema
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
            except Exception:
                self.year_months = None

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
        logging.info(f"{self.logging_string} - Saving data to - {self.parquet_path}...")
        if self.partition_by is None:
            processed_df.write.mode("overwrite").parquet(self.parquet_path)
        else:
            if self.year_months is not None:
                (
                    processed_df.filter(F.col(self.incremental_column).isin(self.year_months))
                    .write.partitionBy(*self.partition_by)
                    .mode("overwrite")
                    .parquet(self.parquet_path)
                )
            else:
                processed_df.write.partitionBy(*self.partition_by).mode("overwrite").parquet(
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
