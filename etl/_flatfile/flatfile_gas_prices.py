def process_data(self, input_tables):
    import sys, subprocess, importlib, site, io, logging
    import pandas as pd
    import requests
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    logging.info(f"{self.logging_string} - Initializing processing...")

    def ensure_xlrd_available():
        try:
            import xlrd  # noqa: F401
            return True
        except Exception:
            pass
        # Try to install and expose on sys.path
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd>=2.0.1"])
        except subprocess.CalledProcessError as e:
            logging.warning(f"pip install xlrd failed: {e}")
            return False

        # Make sure newly installed site-packages are visible
        try:
            user_sp = site.getusersitepackages()
            for p in [user_sp, "/usr/local/lib/python3.9/site-packages", "/usr/local/lib64/python3.9/site-packages"]:
                if p and p not in sys.path:
                    sys.path.append(p)
            importlib.invalidate_caches()
            import xlrd  # noqa: F401
            return True
        except Exception as e:
            logging.warning(f"xlrd still not importable after install: {e}")
            return False

    def build_df_from_pandas(xslxdf: pd.DataFrame):
        xslxdf["Date"] = pd.to_datetime(xslxdf["Date"]).dt.strftime("%Y-%m-%d")
        df_long = xslxdf.melt(id_vars="Date", var_name="Region", value_name="Price")
        df_long["Region_Clean"] = df_long["Region"].str.extract(r"^Weekly\s+(.*?)\s+All Grades", expand=False)
        df_long = df_long[["Date", "Region", "Price", "Region_Clean"]]
        sdf = self.spark.createDataFrame(df_long)
        sdf = sdf.withColumn("year_month", F.date_format(F.col("Date"), "yyyyMM").cast("int"))

        window_spec = Window.partitionBy("year_month", "Region").orderBy(F.col("Date").desc())
        sdf = (
            sdf.withColumn("row_num", F.row_number().over(window_spec))
               .filter(F.col("row_num") == 1)
               .select(
                   "year_month",
                   F.col("Region").alias("Gasoline_Region"),
                   F.col("Price").alias("Gas_Price")
               )
        )
        return sdf

    # Try to fetch from EIA XLS; on failure, fallback to the landed CSV.
    use_fallback_csv = False
    try:
        if not ensure_xlrd_available():
            raise ImportError("xlrd not available")

        url = "https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls"
        # Network can be slow—give it time. verify=False is intentional per your comment.
        resp = requests.get(url, verify=False, timeout=120)
        resp.raise_for_status()
        xlsx = io.BytesIO(resp.content)

        # EIA sheet name per your code
        xslxdf = pd.read_excel(xlsx, sheet_name="Data 12", skiprows=2, engine="xlrd")
        df = build_df_from_pandas(xslxdf)

    except Exception as e:
        logging.warning(f"Falling back to landed CSV due to: {e}")
        use_fallback_csv = True

    if use_fallback_csv:
        # Trust the landed CSV schema; just shape it like the pandas path would.
        flat = input_tables["flat"]
        # Expect columns: Date, Region, Price (and optional Region_Clean)
        df = (
            flat
            .withColumn("year_month", F.date_format(F.col("Date").cast("date"), "yyyyMM").cast("int"))
        )
        window_spec = Window.partitionBy("year_month", "Region").orderBy(F.col("Date").cast("date").desc())
        df = (
            df.withColumn("row_num", F.row_number().over(window_spec))
              .filter(F.col("row_num") == 1)
              .select(
                  "year_month",
                  F.col("Region").alias("Gasoline_Region"),
                  F.col("Price").alias("Gas_Price")
              )
        )

    return df
