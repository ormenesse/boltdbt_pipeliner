from pyspark.sql import SparkSession
from etl_base import ETLBase
from spark_session import create_spark_session
import importlib
import argparse
import types
import boto3
import yaml
import sys

def load_yaml_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def run_jobs(spark, config_path, stages):
    """
    Run spark jobs by stages from flatfiles to gold layer
    """
    config = load_yaml_config(config_path)
    flatfile_bucket = config['configs']['flatfile_bucket']
    output_bucket   = config['configs']['output_bucket']

    for stage in stages:
        if 'flatfile' in stage:
            for job in config.get('flatfile', []):
                module = importlib.import_module(f"etl._flatfile.{job['module']}")
                etl = ETLBase(
                    spark=spark,
                    layer='flatfile',
                    bucket=flatfile_bucket,
                    input_tables=job['input_tables'],
                    output_table_name=job['output_table_name'],
                    partition_by=job.get('partition_by'),
                    unload=job.get('unload', True),
                    incremental=job.get('incremental',False),
                    catalog="shared_catalog",
                    save_catalog=config['configs']['catalog']
                )
                etl.process_data = types.MethodType(module.process_data, etl)
                etl.run()
        else:
            for job in config.get(stage, []):
                module = importlib.import_module(f"etl.0_bronze.{job['module']}")
                etl = ETLBase(
                    spark=spark,
                    layer=stage,
                    bucket=output_bucket,
                    input_tables=job['input_tables'],
                    output_table_name=job['output_table_name'],
                    partition_by=job.get('partition_by',[]),
                    unload=job.get('unload', True),
                    incremental=job.get('incremental',False),
                    catalog="shared_catalog",
                    save_catalog=config['configs']['catalog']
                )
                etl.process_data = types.MethodType(module.process_data, etl)
                etl.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run ETL medallion stages")
    parser.add_argument('--config', '-c',
                        default='configs/etl_config.yaml',
                        help="Path to YAML config")
    parser.add_argument('--flatfile', action='store_true',
                        help="Run only flatfile jobs")
    parser.add_argument('--bronze', action='store_true',
                        help="Run only bronze jobs")
    parser.add_argument('--silver', action='store_true',
                        help="Run only silver jobs")
    parser.add_argument('--gold',   action='store_true',
                        help="Run only gold jobs")
    parser.add_argument('--domain',   action='store_true',
                        help="Run only gold jobs")
    parser.add_argument('--diamond',   action='store_true',
                        help="Run only gold jobs")
    args = parser.parse_args()

    # determine which stages to run
    requested = []
    layers = load_yaml_config(args.config).get('layers',[])
    for stage in layers:
        if getattr(args, stage):
            requested.append(stage)
    if not requested:
        requested = layers

    spark = create_spark_session()
    run_jobs(spark, args.config, requested)
