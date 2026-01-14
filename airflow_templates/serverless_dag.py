# dags/emr_serverless_nps.py
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
# s3://entergy-bdi-dataeng-code-repo-dev/mwaa/dags/entergy-bdi-etl/m2c_data_analytics/
# ── EMR Serverless details ─────────────────────────────
APPLICATION_NAME = "nps-serverless-770-rpyle1"
APPLICATION_ID = "00fskeamk53gpb09"
AWS_ACCOUNT_ID = "308359645995"
AWS_REGION = "us-east-1"

# You provided "runtime role: EMRServerlessDataScienceRole".
# EMR Serverless needs an **IAM role ARN** for `execution_role_arn`.
# Assuming a standard naming/ARN, that becomes:
EXECUTION_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/EMRServerlessDataSciRole"

# NOTE: You also shared a "role ID" that looks like an application ARN:
# arn:aws:emr-serverless:us-east-1:308359645995:/applications/00fske7dcu8gk409
# That's NOT an IAM role ARN, so we won't use it here.

default_args = {
    "owner": "data-eng",
    "retries": 0,
}

spark_submit_params = " ".join([
    # Equivalent to .appName(env)
    "--name DataMart",

    # Driver / executor sizing
    "--conf spark.driver.memory=27G",
    "--conf spark.executor.memory=27G",
    "--conf spark.driver.cores=4",
    "--conf spark.executor.cores=4",

    # Dynamic allocation
    "--conf spark.dynamicAllocation.enabled=true",
    "--conf spark.dynamicAllocation.shuffleTracking.enabled=true",
    "--conf spark.dynamicAllocation.minExecutors=1",
    "--conf spark.dynamicAllocation.initialExecutors=2",
    "--conf spark.dynamicAllocation.maxExecutors=20",

    # Shuffle / partitions
    "--conf spark.sql.shuffle.partitions=1000",

    # S3 upload tuning
    "--conf spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer",
    "--conf spark.hadoop.fs.s3a.fast.upload=true",
    "--conf spark.hadoop.fs.s3a.committer.name=directory",
    "--conf spark.hadoop.fs.s3a.committer.staging.tmp.path=/tmp/spark-s3a-staging",
    "--conf spark.hadoop.fs.s3a.multiobjectdelete.enable=false"
])

with DAG(
    dag_id="emr_serverless_nps_job_bronze_layer",
    start_date=datetime(2025, 1, 1),
    schedule=None,           # run on demand; set a cron if needed
    catchup=False,
    default_args=default_args,
    tags=["emr-serverless", APPLICATION_NAME, AWS_REGION],
) as dag:
    
    run_spark = EmrServerlessStartJobOperator(
        task_id="run_nps_job_bronze_test",
        application_id=APPLICATION_ID,
        execution_role_arn=f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/EMRServerlessDataSciRole",

        # Give the EMR Serverless *job run* a name
        name="nps-serverless-770-vormene-from-airflow-bronze",

        # If your provider version supports it, you can keep this line.
        # Otherwise, remove it and set region in the AWS connection extras (see below).
        # region_name="us-east-1",

        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://entergy-bdi-customer-nps-sensitivity-dev/code_airflow/bronze.py",
                "sparkSubmitParameters": spark_submit_params,  # from earlier
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": "s3://entergy-bdi-customer-nps-sensitivity-dev/code_airflow/run_logs/"}
            },
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=1200,
    )

    run_spark