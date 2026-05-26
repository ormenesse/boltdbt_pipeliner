import logging
import datetime as dt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_data(self, input_tables):
    logging.info(f"{self.logging_string} - Initializing processing...")
    
    def compute_rolling(df_filtered):
        result = df_filtered

        window_spec = Window.partitionBy("state_code") \
                    .orderBy("year_month")
                    
        for months in [3, 6, 12]:        
            result = result.withColumn(
                f"cpi_avg_{months}m",
                F.avg("cpi_value").over(window_spec.rowsBetween(-months + 1, 0))
            ).withColumn(
                f"cpi_change_pct_{months}m",
                ((F.col("cpi_value") - F.lag("cpi_value", months).over(window_spec)) / F.lag("cpi_value", months).over(window_spec) * 100)
            ).withColumn(
                f"gas_price_avg_{months}m",
                F.avg("gas_price").over(window_spec.rowsBetween(-months + 1, 0))
            ).withColumn(
                f"gas_price_change_pct_{months}m",
                ((F.col("gas_price") - F.lag("gas_price", months).over(window_spec)) / F.lag("gas_price", months).over(window_spec) * 100)
            ).withColumn(
                f"inflation_rate_{months}m",
                F.avg("inflation_rate").over(window_spec.rowsBetween(-months + 1, 0))
            ).withColumn(
                f"inflation_rate_pct_{months}m",
                ((F.col("inflation_rate") - F.lag("inflation_rate", months).over(window_spec)) / F.lag("inflation_rate", months).over(window_spec) * 100)
            )
        return result
    
    states = input_tables["states"]
    cpi = input_tables["cpi"]
    gas = input_tables["gas"]
    today = dt.date.today()
    start = (today.replace(day=1) - dt.timedelta(days=4*365))
    month_seq = [(start.year * 100 + start.month)]

    while month_seq[-1] < (today.year * 100 + today.month):
        y, m = divmod(month_seq[-1], 100)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        month_seq.append(y * 100 + m)

    month_df = self.spark.createDataFrame([(m,) for m in month_seq], ["year_month"])
    window_spec = Window.partitionBy("State").orderBy(F.col("year_month"))
    states_id = states.select("State").distinct()
    full_grid = states_id.crossJoin(month_df)
    full_grid = full_grid.join(
        states,
        on=["State"],
        how="left"
    ).join(
        cpi,
        on=["year_month","Region"]
    ).join(
        gas,
        on=["year_month","Gasoline_Region"]
    ).select(
        "year_month","State","CPI","Gas_Price"
    ).withColumn(
        "inflation_rate",
        (F.col("CPI") - F.lag("CPI", 1).over(window_spec)) / F.lag("CPI", 1).over(window_spec) * 100
    ).withColumnRenamed("State", "state_code"
    ).withColumnRenamed("Gas_Price", "gas_price"
    ).withColumnRenamed("CPI", "cpi_value"
    )

    final_df = compute_rolling(full_grid)

    return final_df.drop_duplicates()