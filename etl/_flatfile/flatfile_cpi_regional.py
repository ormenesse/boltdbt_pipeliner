# import logging
# import datetime as dt
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# def process_data(self, input_tables):
#     logging.info(f"{self.logging_string} - Initializing processing...")
#     df = input_tables['flat'].select(
#         input_tables['flat'].columns
#     )

#     df = df.withColumn("year_month", F.date_format(F.col("MonthYear"), "yyyyMM").cast('int'))

#     # Step 2: Define a window partitioned by year_month and Region_Clean, ordered by Date descending
#     window_spec = Window.partitionBy("year_month", "Region").orderBy(F.col("MonthYear").desc())

#     # Step 3: Add a row_number and filter for the first (latest) row
#     df = (
#         df.withColumn("row_num", F.row_number().over(window_spec))
#                 .filter(F.col("row_num") == 1)
#                 .select("year_month", "Region", "Value")
#     ).withColumnsRenamed(
#         { "Value" : "CPI" }
#     )

#     return df

# if firewall is opened change the process_data function to this below:
import requests
import pandas as pd
from datetime import datetime
from time import sleep
import logging

def process_data(self, input_tables):
    logging.info(f"{self.logging_string} - Initializing processing...")
    BLS_API_KEY = "a619c1aee6b54de2921cdfb5e252f02c"
    headers = {'Content-type': 'application/json'}

    region_series_map = {
        'U.S.': 'CUUR0000SA0',
        'South': 'CUUR0300SA0',
        'West': 'CUUR0400SA0',
        'Midwest': 'CUUR0200SA0',
        'South Atlantic': 'CUUR0350SA0',
        'Mountain': 'CUUR0480SA0',
        'East North Central': 'CUUR0230SA0',
        'New England': 'CUUR0110SA0',
        'East South Central': 'CUUR0360SA0',
        'Pacific': 'CUUR0490SA0',
        'West North Central': 'CUUR0240SA0',
        'Middle Atlantic': 'CUUR0120SA0',
        'West South Central': 'CUUR0370SA0'
    }

    start_year = 2021
    end_year = datetime.today().year

    def fetch_bls_series(series_ids):
        all_data = []
        for region, series_id in series_ids.items():
            payload = {
                "seriesid": [series_id],
                "startyear": str(start_year),
                "endyear": str(end_year),
                "registrationkey": BLS_API_KEY
            }
            try:
                r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json=payload, headers=headers, verify=False)
                r.raise_for_status()
                json_data = r.json()

                # Check if valid series data exists
                if 'Results' in json_data and 'series' in json_data['Results'] and json_data['Results']['series']:
                    data = json_data['Results']['series'][0]['data']
                    for entry in data:
                        if entry['period'].startswith('M'):
                            all_data.append({
                                'MonthYear': f"{entry['year']}-{entry['period'][1:]:0>2}-01",
                                'Region': region,
                                'CPI': float(entry['value'])
                            })
                else:
                    print(f"⚠️ No data for {region} (series: {series_id})")
            except Exception as e:
                print(f"❌ Error fetching {region} ({series_id}): {e}")

            sleep(1)  # Respect BLS rate limits

        return pd.DataFrame(all_data)


    def compute_yoy_inflation(df):
        df['MonthYear'] = pd.to_datetime(df['MonthYear'])
        df = df.sort_values(['Region', 'MonthYear'])
        df['Value'] = df.groupby('Region')['CPI'].pct_change(12) * 100
        return df[['MonthYear', 'Region', 'Value']].dropna().sort_values(['Region', 'MonthYear'])

    # Run this to generate the outputs 
    raw_df = fetch_bls_series(region_series_map)
    pandas_df = compute_yoy_inflation(raw_df)
    pandas_df['MonthYear'] = pandas_df['MonthYear'].dt.strftime('%Y-%m-%d')
    
    #transforming final_df into a spark dataframe
    df = self.spark.createDataFrame(pandas_df)
    df = df.withColumn("year_month", F.date_format(F.col("MonthYear"), "yyyyMM").cast('int'))

    # Step 2: Define a window partitioned by year_month and Region_Clean, ordered by Date descending
    window_spec = Window.partitionBy("year_month", "Region").orderBy(F.col("MonthYear").desc())

    # Step 3: Add a row_number and filter for the first (latest) row
    df = (
        df.withColumn("row_num", F.row_number().over(window_spec))
                .filter(F.col("row_num") == 1)
                .select("year_month", "Region", "Value")
    ).withColumnsRenamed(
        { "Value" : "CPI" }
    )

    return df