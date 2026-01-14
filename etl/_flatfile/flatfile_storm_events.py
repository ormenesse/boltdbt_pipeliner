# import logging

# def process_data(self, input_tables):
#     logging.info(f"{self.logging_string} - Initializing processing...")
#     result_df = input_tables['storm_events'].select(
#         input_tables['storm_events'].columns
#     )
#     return result_df

# if firewall is opened, change this code to the following:
import logging
import re
import requests
import pandas as pd
import gzip
from io import BytesIO, StringIO

def process_data(self, input_tables):
    logging.info(f"{self.logging_string} - Initializing processing...")
    
    # Base URL for NOAA storm event CSV files
    BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

    # Step 1: Fetch directory listing
    response = requests.get(BASE_URL)
    response.raise_for_status()
    # Step 2: getting the files name
    matches = re.findall("StormEvents_details-ftp_v1.0_d\d{4}_c\d{8}.csv.gz",response.text)
    # Step 3: downloading the files
    matches = sorted(list(set(matches)))

    storm_dataframe = pd.DataFrame()
    for filename in matches[-2:]:
        print(f"Downloading {filename} ...")
        file_url = BASE_URL + filename
        gz_response = requests.get(file_url)
        gz_response.raise_for_status()
        
        # Decompress in memory
        with gzip.GzipFile(fileobj=BytesIO(gz_response.content)) as gz:
            csv_content = gz.read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            storm_dataframe = pd.concat([storm_dataframe,df],axis=0)

    return self.spark.createDataFrame(storm_dataframe)