"""Example Airflow DAG for running a bolt_pipeliner layer on AWS EMR Serverless.

This file is a reference: copy it into your project's `dags/` directory and
edit APPLICATION_ID / EXECUTION_ROLE_ARN / ENTRY_POINT to match your AWS
account. Other Airflow backends (Databricks, vanilla k8s, in-process) follow
the same shape — see the BashOperator example under
src/bolt_pipeliner/templates/airflow/emrcontaineroperator.txt.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# === Configure for your environment ==========================================
APPLICATION_ID = "REPLACE_WITH_EMR_SERVERLESS_APPLICATION_ID"
EXECUTION_ROLE_ARN = "arn:aws:iam::REPLACE_ACCOUNT_ID:role/REPLACE_ROLE_NAME"
ENTRY_POINT = "s3://your-bucket/code/bronze_entry.py"
LOG_URI = "s3://your-bucket/logs/"
LAYER = "bronze"
# =============================================================================

default_args = {
    "owner": "bolt_pipeliner",
    "retries": 0,
}

# Reasonable defaults; tune via configs/spark/<profile>.toml in your project.
spark_submit_params = " ".join(
    [
        "--name bolt_pipeliner",
        "--conf spark.driver.memory=8G",
        "--conf spark.executor.memory=8G",
        "--conf spark.driver.cores=2",
        "--conf spark.executor.cores=4",
        "--conf spark.dynamicAllocation.enabled=true",
        "--conf spark.dynamicAllocation.minExecutors=1",
        "--conf spark.dynamicAllocation.maxExecutors=10",
        "--conf spark.sql.shuffle.partitions=200",
        "--conf spark.hadoop.fs.s3a.fast.upload=true",
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
    ]
)

with DAG(
    dag_id=f"bolt_pipeliner_emr_serverless_{LAYER}",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["bolt_pipeliner", "emr-serverless", LAYER],
) as dag:
    run_layer = EmrServerlessStartJobOperator(
        task_id=f"run_{LAYER}",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        name=f"bolt_pipeliner_{LAYER}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": ENTRY_POINT,
                "sparkSubmitParameters": spark_submit_params,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": LOG_URI}
            },
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=1200,
    )
