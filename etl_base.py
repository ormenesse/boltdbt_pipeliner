import datetime as dt
import logging
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)

class ETLBase:
    """
    ETL base for Spark + Iceberg (Glue).
    Rules:
      - flatfile: read CSVs from s3a://<bucket>/<path>
      - bronze: DO NOT CHANGE (SQL from self.catalog as in original)
      - else: read Iceberg from save_catalog.datamart.<table>
      - writes: save_catalog.datamart.<layer>_<output_table_name>
    """

    FIXED_SCHEMA = "cxdw_dm"

    def __init__(
        self,
        spark,
        layer,
        bucket,                  # used for flatfile inputs
        input_tables,            # dict or None
        output_table_name,
        partition_by=None,
        unload=True,
        incremental=True,
        catalog="shared_catalog",   # read catalog for bronze (unchanged)
        save_catalog="dev_catalog", # destination catalog + read catalog for non-bronze
        **kwargs,  # extra args accepted without errors
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
        # Output Iceberg identifier: <save_catalog>.datamart.<layer>_<table>
        self.iceberg_table = f"{self.save_catalog}.{self.FIXED_SCHEMA}.{self.layer}_{self.output_table_name}"
        self.logging_string = f"{self.layer} {self.output_table_name}"
        self.table_exists = self._table_exists(self.iceberg_table)

    # ---------------------------
    # Helpers
    # ---------------------------

    def _table_exists(self, table_ident: str) -> bool:
        try:
            return self.spark.catalog.tableExists(table_ident)
        except Exception:
            return False

    def _ensure_namespace(self, catalog: str, schema: str):
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")

    # ---------------------------
    # Incremental window finder
    # ---------------------------

    def check_if_tables_exists_find_yearmonths(self):
        if not self.incremental:
            self.year_months = None
            return
        try:
            if self._table_exists(self.iceberg_table):
                today = dt.date.today()
                start = (today.replace(day=1) - dt.timedelta(days=31*3))
                month_seq = [start.year * 100 + start.month]
                while month_seq[-1] < (today.year * 100 + today.month):
                    y, m = divmod(month_seq[-1], 100)
                    if m == 12:
                        y += 1; m = 1
                    else:
                        m += 1
                    month_seq.append(y*100 + m)
                self.year_months = month_seq
            else:
                self.year_months = None
        except Exception:
            self.year_months = None

    # ---------------------------
    # IO: Load / Save
    # ---------------------------

    def load_data(self, input_path=None):
        """
        - flatfile: s3a://<bucket>/<path>
        - bronze: (UNCHANGED) SELECT * FROM <self.catalog>.<table_name_given>
        - else: read.table(<save_catalog>.datamart.<table_name_given>)
        """
        print(f"{self.logging_string} - Loading data...")

        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                if '.csv' in rel_path:
                    self.input_tables[key] = self.spark.read.csv(
                        f"{self.bucket}/{rel_path}",
                        header=True, inferSchema=True, multiLine=True,
                        escape='"', quote='"'
                    )
                elif '.parquet' in rel_path:
                    self.input_tables[key] = self.spark.read.parquet(
                        f"{self.bucket}/{rel_path}"
                    )
                else:
                    print(f"{self.logging_string} - Could not flatfile - {rel_path}")
                print(f"{self.logging_string} - Loaded flatfile - {rel_path}")
            return

        if self.layer == "bronze":
            # DO NOT CHANGE: use the original SQL against self.catalog
            for key in self.input_table_names.keys():
                if '.' in self.input_table_names[key]:
                    self.input_tables[key] = self.spark.sql(
                        f"""
                            SELECT *
                            FROM {self.catalog}.{self.input_table_names[key]}
                        """
                    )
                else:
                    table_ident = f"{self.save_catalog}.{self.FIXED_SCHEMA}.{self.input_table_names[key]}"
                    self.input_tables[key] = self.spark.read.table(table_ident)    
                print(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            return

        # Else (silver/gold/etc): read from save_catalog + FIXED_SCHEMA
        for key, name in self.input_table_names.items():
            table_ident = f"{self.save_catalog}.{self.FIXED_SCHEMA}.{name}"
            self.input_tables[key] = self.spark.read.table(table_ident)
            print(f"{self.logging_string} - Loaded - {table_ident}")
    
    def _create_table(self, df):
        writer = df.writeTo(self.iceberg_table)
        if self.partition_by:
            # Use Column objects for better compatibility
            writer = writer.partitionedBy(*[F.col(c) for c in self.partition_by])
        writer.createOrReplace()

    def _replace_table_partitions(self, df):
        df.writeTo(self.iceberg_table).overwritePartitions()

    def unload_data(self, processed_df):
        processed_df.cache()
        print(f"{self.logging_string} - Saving data to Iceberg table - {self.iceberg_table}...")

        # Ensure namespace exists before writing
        self._ensure_namespace(self.save_catalog, self.FIXED_SCHEMA)

        if not self.table_exists:
            # First write → CREATE
            self._create_table(processed_df)
        else:
            # Table exists
            if self.incremental and self.year_months is not None:
                target_df = processed_df.filter(F.col("year_month").isin(self.year_months))
                self._replace_table_partitions(target_df)
            else:
                # Full refresh; if table is partitioned, this is fine and efficient.
                # If you ever make it unpartitioned, prefer:
                # processed_df.write.mode("overwrite").saveAsTable(self.iceberg_table)
                self._replace_table_partitions(processed_df)

        print(f"{self.logging_string} - Data successfully saved to Iceberg - {self.iceberg_table}")

    # ---------------------------
    # ETL / Run
    # ---------------------------

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