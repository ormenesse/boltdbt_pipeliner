import sys
import random
import numpy as np
import datetime as dt
from decimal import Decimal
import datetime as dt
from itertools import product
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import boto3
import logging
import types
logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("prod")
        # congiure your spark session here.
        # Partition configurations
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        .getOrCreate()
    )

    spark.sql("use dev_catalog")
    spark.sql("use shared_catalog")

    return spark
