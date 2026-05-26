import datetime as dt
import logging

logging.basicConfig(level=logging.INFO)


class ETLBaseDelta:
    """
    ETL base for Spark on Synapse using Delta tables only.

    Rules:
      - flatfile: read CSVs/Parquet from <bucket>/<path>
      - bronze: full SQL (e.g. "SELECT * FROM `esm`.`invoice`") or catalog.table
      - else: read tables from <save_catalog>.<table>
      - writes: Delta to output_table
    """

    def __init__(
        self,
        spark,
        layer,
        bucket="",
        input_tables=None,
        output_table_name=None,
        partition_by=None,
        unload=True,
        incremental=True,
        catalog="shared_catalog",
        save_catalog="dev_catalog",
        output_table=False,
        output_format="delta",
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
        self.year_months = None
        self.output_table_name = output_table_name
        self.output_table = output_table
        self.output_format = output_format
        self.logging_string = f"{self.layer} {self.output_table_name}"
        self._write_table = self.save_catalog + "." + self.layer + "_" + self.output_table_name
        self.table_exists = self._table_exists(self._write_table)

    def _table_exists(self, table_ident: str) -> bool:
        try:
            return self.spark.catalog.tableExists(table_ident)
        except Exception:
            return False

    def check_if_tables_exists_find_yearmonths(self):
        if not self.incremental:
            self.year_months = None
            return
        try:
            if self._table_exists(self._write_table):
                today = dt.date.today()
                start = today.replace(day=1) - dt.timedelta(days=31 * 3)
                month_seq = [start.year * 100 + start.month]
                while month_seq[-1] < (today.year * 100 + today.month):
                    y, m = divmod(month_seq[-1], 100)
                    if m == 12:
                        y += 1
                        m = 1
                    else:
                        m += 1
                    month_seq.append(y * 100 + m)
                self.year_months = month_seq
            else:
                self.year_months = None
        except Exception:
            self.year_months = None

    def load_data(self, input_path=None):
        print(f"{self.logging_string} - Loading data...")

        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                if ".csv" in rel_path:
                    self.input_tables[key] = self.spark.read.csv(
                        f"{self.bucket}/{rel_path}",
                        header=True,
                        inferSchema=True,
                        multiLine=True,
                        escape='"',
                        quote='"',
                    )
                elif ".parquet" in rel_path:
                    self.input_tables[key] = self.spark.read.parquet(
                        f"{self.bucket}/{rel_path}"
                    )
                else:
                    print(f"{self.logging_string} - Could not flatfile - {rel_path}")
                print(f"{self.logging_string} - Loaded flatfile - {rel_path}")
            return

        if self.layer == "bronze":
            for key in self.input_table_names.keys():
                self.input_tables[key] = self.spark.sql(
                    f"""
                        SELECT *
                        FROM {self.input_table_names[key]}
                    """
                )
                logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            return

        for key, name in self.input_table_names.items():
            table_ident = f"{self.save_catalog}.{name}"
            self.input_tables[key] = self.spark.sql(
                f"""
                    SELECT *
                    FROM {self.save_catalog}.{self.input_table_names[key]}
                """
            )
            print(f"{self.logging_string} - Loaded - {table_ident}")

    def _write_delta(self, processed_df):
        if self.incremental:
            mode = "append" if self._table_exists(self._write_table) else "overwrite"
        else:
            mode = "overwrite"

        if mode == "overwrite":
            writer = (
                processed_df.write.mode(mode)
                .option("overwriteSchema", "true")
                .format("delta")
            )
        else:
            writer = processed_df.write.mode(mode).format("delta")

        if mode == "overwrite" and self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        writer.saveAsTable(self._write_table)

    def unload_data(self, processed_df):
        processed_df.cache()
        print(f"{self.logging_string} - Saving data to Delta table - {self._write_table}...")
        self._write_delta(processed_df)
        print(f"{self.logging_string} - Data successfully saved to Delta - {self._write_table}")

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

        if self.output_table:
            self.df.show(20)
