from pyspark.sql import functions as F
import datetime as dt
import logging
import types
import datetime as dt
import logging
from typing import Dict, Optional, Iterable, Any
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

try:
    import fsspec  # needed for S3/local/other filesystems
except ImportError:
    fsspec = None

logging.basicConfig(level=logging.INFO)

class ETLBaseParquet:
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
        **kwargs
    ):
        self.spark = spark
        self.layer = layer
        self.input_table_names = input_tables
        self.output_table_name = output_table_name
        self.bucket = bucket
        self.input_tables = {}
        self.parquet_path = f"s3a://{bucket}/{self.layer}_{output_table_name}.parquet"
        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = partition_by
        self.df = None
        self.incremental = incremental
        self.year_months = None
        self.unload = unload
        if self.layer == 'flatfile':
            self.parquet_path = self.parquet_path.replace('flat_files','data')
    
    def check_if_tables_exists_find_yearmonths(self):
        """
        Function for incremental load.
        """
        if self.incremental:
            try:
                schema = self.spark.read.parquet(self.parquet_path).printSchema()
                del schema
                today = dt.date.today()
                start = (today.replace(day=1) - dt.timedelta(days=31*3))
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
            except:
                self.year_months = None

    def load_data(self, input_path):
        """
            Loading Data Files 
        """
        logging.info(f"{self.logging_string} - Loading data...")
        if self.input_table_names is not None:
            if self.layer == 'bronze':
                for key in self.input_table_names.keys():
                    self.input_tables[key] = self.spark.sql(
                        f"""
                            SELECT *
                            FROM shared_catalog.{self.input_table_names[key]}
                        """
                    )
                    logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            elif self.layer == 'flatfile':
                for key in self.input_table_names.keys():
                    self.input_tables[key] = self.spark.read.csv(
                        f"s3a://{self.bucket}/{self.input_table_names[key]}",
                        header=True, inferSchema=True, multiLine=True,
                        escape='"', quote='"' 
                    )
                    logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            else:
                for key in self.input_table_names.keys():
                    self.input_tables[key] = \
                        self.spark.read.parquet(f"s3a://{self.bucket}/{self.input_table_names[key]}.parquet")
                    logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            
    def unload_data(self, processed_df):
        """
            Saving 2 Parquet
        """
        processed_df.cache()
        logging.info(f"{self.logging_string} - Saving data to - {self.parquet_path}...")
        if self.partition_by == None:
            processed_df.write.mode("overwrite").parquet(self.parquet_path)
        else:
            if self.year_months is not None:
                processed_df.filter(F.col("yearMonth").isin(self.year_months)).write.partition_by(self.partition_by).mode("overwrite").parquet(self.parquet_path)
            else:    
                processed_df.write.partition_by(self.partition_by).mode("overwrite").parquet(self.parquet_path)
        logging.info(f"{self.logging_string} - Data sucessfully saved to - {self.parquet_path}")

    def process_data(self, dfs):
        """
            ETL Here
        """
        logging.info(f"{self.logging_string} - Initializing processing...")
        raise NotImplementedError("This method should be overridden by subclasses.")
        
    def run(self):
        """
            Run function
        """
        self.check_if_tables_exists_find_yearmonths()
        # Load data
        self.load_data(self.input_table_names)
        
        # Process data using the dynamically attached method
        if hasattr(self, 'process_data'):
            processed_df = self.process_data(self.input_tables)
            self.df = processed_df
        else:
            raise NotImplementedError("No process_data method defined.")
        
        # Unload data
        if self.unload:
            self.unload_data(self.df)

import datetime as dt
import logging
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)

class ETLBaseIceberg:
    """
    ETL base for Spark + Iceberg (Glue).
    Rules:
      - flatfile: read CSVs from s3a://<bucket>/<path>
      - bronze: DO NOT CHANGE (SQL from self.catalog as in original)
      - else: read Iceberg from save_catalog.datamart.<table>
      - writes: save_catalog.datamart.<layer>_<output_table_name>
    """

    FIXED_SCHEMA = "etrdatamart"

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
        **kwargs
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
        logging.info(f"{self.logging_string} - Loading data...")

        if not self.input_table_names:
            return

        if self.layer == "flatfile":
            for key, rel_path in self.input_table_names.items():
                self.input_tables[key] = self.spark.read.csv(
                    f"s3a://{self.bucket}/{rel_path}",
                    header=True, inferSchema=True, multiLine=True,
                    escape='"', quote='"'
                )
                logging.info(f"{self.logging_string} - Loaded flatfile - {rel_path}")
            return

        if self.layer == "bronze":
            # DO NOT CHANGE: use the original SQL against self.catalog
            for key in self.input_table_names.keys():
                self.input_tables[key] = self.spark.sql(
                    f"""
                        SELECT *
                        FROM {self.catalog}.{self.input_table_names[key]}
                    """
                )
                logging.info(f"{self.logging_string} - Loaded - {self.input_table_names[key]}")
            return

        # Else (silver/gold/etc): read from save_catalog + FIXED_SCHEMA
        for key, name in self.input_table_names.items():
            table_ident = f"{self.save_catalog}.{self.FIXED_SCHEMA}.{name}"
            self.input_tables[key] = self.spark.read.table(table_ident)
            logging.info(f"{self.logging_string} - Loaded - {table_ident}")

    def _create_or_replace_table(self, df):
        writer = df.writeTo(self.iceberg_table)
        if self.partition_by:
            writer = writer.partitionedBy(*self.partition_by)
        writer.createOrReplace()

    def _overwrite_partitions(self, df):
        df.writeTo(self.iceberg_table).overwritePartitions()

    def unload_data(self, processed_df):
        processed_df.cache()
        logging.info(f"{self.logging_string} - Saving data to Iceberg table - {self.iceberg_table}...")

        # Ensure namespace exists before writing
        self._ensure_namespace(self.save_catalog, self.FIXED_SCHEMA)

        table_exists = self._table_exists(self.iceberg_table)

        if self.incremental and self.year_months is not None and table_exists:
            target_df = processed_df.filter(F.col("yearMonth").isin(self.year_months))
            self._overwrite_partitions(target_df)
        else:
            self._create_or_replace_table(processed_df)

        logging.info(f"{self.logging_string} - Data successfully saved to Iceberg - {self.iceberg_table}")

    # ---------------------------
    # ETL / Run
    # ---------------------------

    def process_data(self, dfs):
        logging.info(f"{self.logging_string} - Initializing processing...")
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

import datetime as dt
import logging
from typing import Dict, Optional, Iterable, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

try:
    import fsspec  # needed for S3/local/other filesystems
except ImportError:
    fsspec = None


class ETLBaseParquetPandas:
    """
    Pandas/pyarrow ETL base:
      - No Spark or SQL.
      - Input: CSV or Parquet (local or S3 paths).
      - Output: Parquet (optionally partitioned).
      - Incremental: process only the last 3 months if an existing output dataset already exists.
    """

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
        **kwargs
    ):
        """
        Args:
            layer: e.g., "bronze", "silver", "gold", "flatfile"
            bucket: for convenience when building S3 paths; may be None if you pass absolute paths in input_tables
            input_tables: mapping {alias: path}. Path may be local or S3; if it ends with ".csv" it will be read as CSV, else Parquet.
            output_table_name: will become dataset folder name like s3://{bucket}/{layer}_{name}/
            partition_by: list/tuple of column names to partition on when writing parquet (Spark-style dirs)
            unload: if True, writes the processed dataframe
            incremental: if True, restricts to recent 3 months *when* an output dataset already exists
            storage_options: passed to pandas/pyarrow/fsspec (e.g., {"anon": False} or AWS creds)
        """
        self.layer = layer
        self.input_table_names = input_tables
        self.output_table_name = output_table_name
        self.bucket = bucket
        self.input_tables: Dict[str, pd.DataFrame] = {}
        # Spark used "<layer>_<name>.parquet" but wrote a directory. We'll mirror that as a dataset directory:
        base = f"{bucket}/" if bucket else ""
        self.dataset_path = f"{base}{self.layer}_{self.output_table_name}"
        # maintain your special-case path tweak
        if self.layer == "flatfile":
            self.dataset_path = self.dataset_path.replace("flat_files", "data")

        self.logging_string = f"{layer} {output_table_name}"
        self.partition_by = tuple(partition_by) if partition_by else None
        self.df: Optional[pd.DataFrame] = None
        self.incremental = incremental
        self.year_months: Optional[list[int]] = None
        self.unload = unload
        self.storage_options = storage_options or {}

    # ---------- helpers ----------

    def _fs_exists(self, path: str) -> bool:
        """Check if a file/directory exists for local/S3/etc using fsspec if available."""
        if fsspec is None:
            # Fallback: pyarrow FS (works well for S3/local if configured via env)
            try:
                pafs, pafspath = pa.fs.FileSystem.from_uri(path)
                return pafs.exists(pafspath)
            except Exception:
                return False
        else:
            fs, _, paths = fsspec.get_fs_token_paths(path, storage_options=self.storage_options)
            return fs.exists(paths[0])

    def _list_any_parquet_file(self, path: str) -> bool:
        """Heuristic to detect an existing parquet dataset folder."""
        try:
            dataset = ds.dataset(path, format="parquet", filesystem=None)
            # Will succeed if there is a valid dataset; len(files) > 0
            _ = list(dataset.files)
            return True
        except Exception:
            return False

    # ---------- incremental ----------

    def check_if_tables_exists_find_yearmonths(self):
        """
        If incremental is enabled and the output dataset already exists,
        compute the year-month integers for the last 3 full months + current month.
        """
        if not self.incremental:
            self.year_months = None
            return

        exists = self._fs_exists(self.dataset_path) and self._list_any_parquet_file(self.dataset_path)
        if not exists:
            self.year_months = None
            return

        today = dt.date.today()
        start = (today.replace(day=1) - dt.timedelta(days=31 * 3))
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

    # ---------- IO ----------

    def load_data(self):
        """
        Load input tables (CSV/Parquet) into pandas DataFrames.
        - If a path ends with .csv -> read CSV
        - Else -> read Parquet
        """
        logging.info(f"{self.logging_string} - Loading data...")
        if not self.input_table_names:
            return

        for key, path in self.input_table_names.items():
            # If the caller only provided a table stem, allow constructing path from layer/bucket
            # but prefer absolute paths provided by the caller.
            if not (path.startswith("s3://") or path.startswith("/") or path.endswith(".csv") or path.endswith(".parquet")) and self.bucket:
                # Assume a parquet dataset by default
                path = f"s3://{self.bucket}/{path}"

            if path.lower().endswith(".csv"):
                df = pd.read_csv(path, storage_options=self.storage_options)
            else:
                # Parquet: pandas can read a file or a directory (dataset) when engine='pyarrow'
                df = pd.read_parquet(path, storage_options=self.storage_options)
            self.input_tables[key] = df
            logging.info(f"{self.logging_string} - Loaded - {key} from {path}")

    def unload_data(self, processed_df: pd.DataFrame):
        """
        Save DataFrame to Parquet.
        - If `partition_by` is provided -> write partitioned dataset (Spark-style folders).
        - Else -> write a non-partitioned dataset (single folder of parquet files).
        Notes:
          * pyarrow.dataset.write_dataset will create/overwrite the target directory.
          * If incremental + self.year_months is set and 'yearMonth' is present,
            restrict write to those months.
        """
        if processed_df is None or processed_df.empty:
            logging.info(f"{self.logging_string} - Nothing to write (empty df).")
            return

        df_to_write = processed_df.copy()

        # Apply incremental month filter if available and column exists
        if self.year_months is not None and "yearMonth" in df_to_write.columns:
            df_to_write = df_to_write[df_to_write["yearMonth"].isin(self.year_months)]

        logging.info(f"{self.logging_string} - Saving data to - {self.dataset_path} ...")

        # Convert pandas -> pyarrow table
        table = pa.Table.from_pandas(df_to_write, preserve_index=False)

        # Overwrite behavior: delete if exists (common with dataset writers)
        # Use FileSystem to remove existing target before write to mimic Spark overwrite
        try:
            pafs, pafspath = pa.fs.FileSystem.from_uri(self.dataset_path)
            if pafs.exists(pafspath):
                pafs.delete_dir(pafspath)
        except Exception:
            # ignore if path doesn't exist / not deletable
            pass

        if self.partition_by:
            ds.write_dataset(
                data=table,
                base_dir=self.dataset_path,
                format="parquet",
                partitioning=self.partition_by,
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            # Still use write_dataset to produce a parquet dataset directory (unpartitioned)
            ds.write_dataset(
                data=table,
                base_dir=self.dataset_path,
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
            )

        logging.info(f"{self.logging_string} - Data successfully saved to - {self.dataset_path}")

    # ---------- hooks ----------

    def process_data(self, dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Override in subclasses to do the actual transform with pandas.
        Expected to return a pandas.DataFrame.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    # ---------- runner ----------

    def run(self):
        """
        Execute the ETL:
          1) Check incremental window (if any)
          2) Load inputs (CSV/Parquet)
          3) Process via subclass' process_data()
          4) Save to Parquet (optionally partitioned)
        """
        self.check_if_tables_exists_find_yearmonths()
        self.load_data()

        if not hasattr(self, "process_data"):
            raise NotImplementedError("No process_data method defined.")

        processed_df = self.process_data(self.input_tables)
        self.df = processed_df

        if self.unload:
            self.unload_data(self.df)

class ETLBaseParquetPolars:
    """
    Polars/pyarrow ETL base:
      - Input: CSV or Parquet (local or S3 paths).
      - Output: Parquet (optionally partitioned).
      - Incremental: process only the last 3 months if an existing output dataset already exists.
    """

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
        **kwargs,  # extra args accepted without errors
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
        self.extra_args = kwargs  # keep unused args safely

    # ---------- incremental ----------
    def check_if_tables_exists_find_yearmonths(self):
        if not self.incremental:
            self.year_months = None
            return

        try:
            dataset = ds.dataset(self.dataset_path, format="parquet")
            _ = list(dataset.files)  # fails if path doesn’t exist
        except Exception:
            self.year_months = None
            return

        today = dt.date.today()
        start = (today.replace(day=1) - dt.timedelta(days=31 * 3))
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

    # ---------- IO ----------
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

        if self.year_months is not None and "yearMonth" in df_to_write.columns:
            df_to_write = df_to_write.filter(pl.col("yearMonth").is_in(self.year_months))

        logging.info(f"{self.logging_string} - Saving data to - {self.dataset_path}...")

        # Polars doesn’t yet support Spark-style partitioned writes natively.
        # We bridge via PyArrow for partitioned dataset writes:
        table = df_to_write.to_arrow()

        try:
            pafs, pafspath = pa.fs.FileSystem.from_uri(self.dataset_path)
            if pafs.exists(pafspath):
                pafs.delete_dir(pafspath)
        except Exception:
            pass

        if self.partition_by:
            ds.write_dataset(
                data=table,
                base_dir=self.dataset_path,
                format="parquet",
                partitioning=self.partition_by,
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            ds.write_dataset(
                data=table,
                base_dir=self.dataset_path,
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
            )

        logging.info(f"{self.logging_string} - Data successfully saved to - {self.dataset_path}")

    # ---------- hooks ----------
    def process_data(self, dfs: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        raise NotImplementedError("This method should be overridden by subclasses.")

    # ---------- runner ----------
    def run(self):
        self.check_if_tables_exists_find_yearmonths()
        self.load_data()
        if not hasattr(self, "process_data"):
            raise NotImplementedError("No process_data method defined.")
        processed_df = self.process_data(self.input_tables)
        self.df = processed_df
        if self.unload:
            self.unload_data(self.df)