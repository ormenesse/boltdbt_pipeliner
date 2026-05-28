from collections import deque
from typing import Dict, List, Optional, Set
import yaml
import sys
import os

from bolt_pipeliner.generators._paths import (
    ETL_BASE_SOURCE,
    TEMPLATES_AIRFLOW,
    TEMPLATES_DOCS,
)
from bolt_pipeliner.config import resolve_data_locations
from bolt_pipeliner.sessions.profiles import resolve_spark_profile


# Constants
OUTPUT_DIRS = [
    "./outputs",
    "./outputs/airflow",
    "./outputs/airflow/code",
    "./outputs/airflow/dags"
]

TEMPLATE_FILES = {
    'etl_base': str(ETL_BASE_SOURCE),
    'job_script': str(TEMPLATES_DOCS / "job_script.txt"),
    'spark_config': str(TEMPLATES_AIRFLOW / "spark_config.txt"),
    'dag_template': str(TEMPLATES_AIRFLOW / "dag.txt"),
}


RUNTIME_SPECS = {
    "local": {
        "operator_imports": "from airflow.operators.bash import BashOperator",
        "runtime_constants": "",
        "task_template": """{job_script_name} = BashOperator(
|    task_id=\"{job_script_name}\",
|    bash_command=\"python ./outputs/airflow/code/{job_script_name}.py\",
|)""",
    },
    "emr": {
        "operator_imports": "from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator",
        "runtime_constants": """
EMR_APPLICATION_ID = \"REPLACE_WITH_EMR_SERVERLESS_APPLICATION_ID\"
EMR_EXECUTION_ROLE_ARN = \"arn:aws:iam::REPLACE_ACCOUNT_ID:role/REPLACE_ROLE_NAME\"
EMR_CODE_BASE_URI = \"s3://REPLACE_BUCKET/airflow/code\"
EMR_LOG_URI = \"s3://REPLACE_BUCKET/airflow/logs/\"
EMR_SPARK_SUBMIT_PARAMETERS = \"--conf spark.sql.shuffle.partitions=200\"
""".strip(),
        "task_template": """{job_script_name} = EmrServerlessStartJobOperator(
|    task_id=\"{job_script_name}\",
|    application_id=EMR_APPLICATION_ID,
|    execution_role_arn=EMR_EXECUTION_ROLE_ARN,
|    name=\"{job_script_name}\",
|    job_driver={{
|        \"sparkSubmit\": {{
|            \"entryPoint\": f\"{{EMR_CODE_BASE_URI}}/{job_script_name}.py\",
|            \"sparkSubmitParameters\": EMR_SPARK_SUBMIT_PARAMETERS,
|        }}
|    }},
|    configuration_overrides={{
|        \"monitoringConfiguration\": {{
|            \"s3MonitoringConfiguration\": {{\"logUri\": EMR_LOG_URI}}
|        }}
|    }},
|    aws_conn_id=\"aws_default\",
|    wait_for_completion=True,
|)""",
    },
    "gcp": {
        "operator_imports": "from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator",
        "runtime_constants": """
GCP_PROJECT_ID = \"REPLACE_GCP_PROJECT_ID\"
GCP_REGION = \"REPLACE_GCP_REGION\"
DATAPROC_CLUSTER = \"REPLACE_DATAPROC_CLUSTER\"
GCP_CODE_BASE_URI = \"gs://REPLACE_BUCKET/airflow/code\"
""".strip(),
        "task_template": """{job_script_name} = DataprocSubmitJobOperator(
|    task_id=\"{job_script_name}\",
|    project_id=GCP_PROJECT_ID,
|    region=GCP_REGION,
|    gcp_conn_id=\"google_cloud_default\",
|    job={{
|        \"reference\": {{\"project_id\": GCP_PROJECT_ID}},
|        \"placement\": {{\"cluster_name\": DATAPROC_CLUSTER}},
|        \"pyspark_job\": {{
|            \"main_python_file_uri\": f\"{{GCP_CODE_BASE_URI}}/{job_script_name}.py\"
|        }},
|    }},
|)""",
    },
    "azure": {
        "operator_imports": "from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator",
        "runtime_constants": """
AZURE_RESOURCE_GROUP = \"REPLACE_AZURE_RESOURCE_GROUP\"
AZURE_REGION = \"eastus\"
AZURE_CONTAINER_IMAGE = \"REPLACE_IMAGE_WITH_PYSPARK_AND_BOLT\"
AZURE_CODE_BASE_URI = \"https://REPLACE_STORAGE.blob.core.windows.net/airflow/code\"
""".strip(),
        "task_template": """{job_script_name} = AzureContainerInstancesOperator(
|    task_id=\"{job_script_name}\",
|    ci_conn_id=\"azure_default\",
|    resource_group=AZURE_RESOURCE_GROUP,
|    name=\"{job_script_name}\",
|    image=AZURE_CONTAINER_IMAGE,
|    region=AZURE_REGION,
|    command=[\"python\", f\"{{AZURE_CODE_BASE_URI}}/{job_script_name}.py\"],
|)""",
    },
    "k8s": {
        "operator_imports": "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator",
        "runtime_constants": """
K8S_NAMESPACE = \"default\"
K8S_IMAGE = \"REPLACE_IMAGE_WITH_PYSPARK_AND_BOLT\"
""".strip(),
        "task_template": """{job_script_name} = KubernetesPodOperator(
|    task_id=\"{job_script_name}\",
|    namespace=K8S_NAMESPACE,
|    image=K8S_IMAGE,
|    cmds=[\"python\", \"/opt/airflow/dags/code/{job_script_name}.py\"],
|    get_logs=True,
|)""",
    },
    "databricks": {
        "operator_imports": "from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator",
        "runtime_constants": """
DATABRICKS_CODE_BASE_URI = \"dbfs:/FileStore/airflow/code\"
""".strip(),
        "task_template": """{job_script_name} = DatabricksSubmitRunOperator(
|    task_id=\"{job_script_name}\",
|    databricks_conn_id=\"databricks_default\",
|    json={{
|        \"new_cluster\": {{
|            \"spark_version\": \"13.3.x-scala2.12\",
|            \"node_type_id\": \"i3.xlarge\",
|            \"num_workers\": 2,
|        }},
|        \"spark_python_task\": {{
|            \"python_file\": f\"{{DATABRICKS_CODE_BASE_URI}}/{job_script_name}.py\",
|        }},
|    }},
|)""",
    },
}


def load_yaml_config(config_path: str) -> Dict:
    """Load and return YAML configuration from file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def create_output_directories() -> None:
    """Create all necessary output directories."""
    for directory in OUTPUT_DIRS:
        os.makedirs(directory, exist_ok=True)


def extract_config_values(config: Dict) -> tuple:
    """Extract bucket and schema configuration values."""
    layers = config['layers']
    configs = config['configs']
    flatfile_location, output_location = resolve_data_locations(config)
    return (
        flatfile_location,
        output_location,
        configs['schema'],
        layers
    )


def filter_layers(layers: Dict, target_layers: Optional[List[str]]) -> Dict:
    """Filter layers to only include target layers if specified."""
    if not target_layers:
        return layers
    return {k: v for k, v in layers.items() if k in target_layers}


def get_layer_order(layers: Dict) -> List[str]:
    """Get the layer order from the configuration."""
    return list(layers.keys())


def get_previous_layer(current_layer: str, layer_order: List[str]) -> Optional[str]:
    """Get the previous layer in the processing order."""
    try:
        current_index = layer_order.index(current_layer)
        if current_index > 0:
            return layer_order[current_index - 1]
    except ValueError:
        pass
    return None


def normalize_airflow_runtime(profile: str) -> str:
    """Map spark/runtime profiles to Airflow DAG operator families."""
    key = (profile or "").lower()
    if key in {"emr", "glue"}:
        return "emr"
    if key in {"gcp", "dataproc"}:
        return "gcp"
    if key in {"azure"}:
        return "azure"
    if key in {"k8s", "kubernetes"}:
        return "k8s"
    if key in {"databricks"}:
        return "databricks"
    return "local"


def get_runtime_spec(runtime_target: str) -> Dict[str, str]:
    return RUNTIME_SPECS.get(runtime_target, RUNTIME_SPECS["local"])


def has_dependencies_ready(job: Dict, layer: str, completed_jobs: Set[str]) -> bool:
    """Check if all dependencies for a job are ready."""
    input_tables = [str(v) for v in job.get('input_tables', {}).values()]
    if len(input_tables) > 0:
        if layer == "flatfile":
            return True
        return all(
            (table in completed_jobs if layer in table else True)
            for table in input_tables
        )
    return True


def process_job_queue(jobs: List[Dict], layer: str) -> Set[str]:
    """
    Process jobs in dependency order and return set of completed job names.
    Uses a queue-based approach to handle dependencies.
    """
    queue = deque(jobs)
    completed_jobs = []
    stalled_passes = 0
    max_stalls = len(queue) + 1  # Safety cap to prevent infinite loops
    
    while queue and stalled_passes < max_stalls:
        initial_queue_size = len(queue)
        
        for _ in range(initial_queue_size):
            job = queue.popleft()
            job_name = f"{layer}_{job['output_table_name']}"
            if has_dependencies_ready(job, layer, completed_jobs):
                completed_jobs.append(job_name)
            else:
                queue.append(job)  # Put back for next iteration
        
        # Check if we made progress
        if len(queue) == initial_queue_size:
            stalled_passes += 1
        else:
            stalled_passes = 0
    
    if stalled_passes >= max_stalls:
        print("This might be etlconfig missconfiguration.")
        print(f"Warning: Some jobs in layer '{layer}' have circular dependencies")
        
    return completed_jobs


def read_template_file(file_path: str) -> str:
    """Read template file content with error handling."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Warning: Template file '{file_path}' not found")
        return ""


def generate_job_script(job: Dict, layer: str, module_prefix: str, 
                       config_values: tuple) -> Optional[str]:
    """
    Generate individual job script and return job script name if successful.
    """
    flatfile_bucket, output_bucket, _, _ = config_values
    
    # Read all required template files
    templates = {}
    for key, file_path in TEMPLATE_FILES.items():
        templates[key] = read_template_file(file_path)
        if not templates[key] and key != 'etl_base':  # etl_base is optional
            return None
    
    # Build module path
    module_path = f"./{module_prefix}/{job['module']}.py"
    
    try:
        # Read job-specific code
        with open(module_path, 'r', encoding='utf-8') as f:
            job_code = f.read()
    except FileNotFoundError:
        print(f"Warning: Could not find module {module_path}, skipping...")
        return None
    
    # Format job script
    job_script = templates['job_script'].format(
        layer=layer.upper(),
        layer_lower=layer.lower(),
        module=job['module'],
        job=job,
        job_code=job_code,
        module_path=module_path,
        etl_base_code=templates['etl_base'],
        input_tables=job.get('input_tables', None),
        output_table_name=job['output_table_name'],
        partition_by=job.get('partition_by', None),
        unload=job.get('unload', True),
        incremental=job.get('incremental', False)
    )
    
    # Format spark script
    job_script_name = f"{layer}_{job['output_table_name']}"
    spark_script = templates['spark_config'].format(
        etl_base_code=templates['etl_base'],
        code=job_script,
        job_script_name=job_script_name,
        save_catalog=output_bucket,
        flatfile_bucket=flatfile_bucket
    )
    
    # Write job script to file
    output_file = f"./outputs/airflow/code/{job_script_name}.py"
    with open(output_file, 'w', encoding="utf-8") as f:
        f.write(spark_script)
    
    return job_script_name


def generate_dag_script(
    layer: str,
    completed_jobs: List[str],
    config_values: tuple,
    layer_order: List[str],
    runtime_target: str,
) -> None:
    """Generate the main DAG script for a layer with dependencies."""
    _, _, fixed_schema, _ = config_values
    
    # Read DAG templates
    dag_template = read_template_file(TEMPLATE_FILES['dag_template'])
    if not dag_template:
        print(f"Error: Could not read DAG templates for layer '{layer}'")
        return

    runtime_spec = get_runtime_spec(runtime_target)
    
    # Build EMR configuration and task order
    task_operator_code = ""
    tasks_order = ""
    
    for job_name in completed_jobs:
        task_operator_code += "        " + runtime_spec["task_template"].format(
            job_script_name=job_name,
            layer=layer,
            database=fixed_schema
        ).replace("|", "        ") + "\n"
        tasks_order += f"\n        >> {job_name}"
    
    # Generate dependency sensor code if there's a previous layer
    dependency_sensor_code = ""
    previous_layer = get_previous_layer(layer, list(layer_order))
    
    if previous_layer:
    #     dependency_sensor_code = f"""
    # # Wait for previous layer to complete
    # wait_for_{previous_layer} = ExternalTaskSensor(
    #     task_id="wait_for_{previous_layer}",
    #     external_dag_id="boltpipe_{previous_layer}",
    #     external_task_id="TG_boltpipe_{previous_layer}",
    #     timeout=3600*12,  # 12 hour timeout
    #     poke_interval=60,  # Check every minute
    #     mode="reschedule",
    #     allowed_states=[DagRunState.SUCCESS],
    # )"""
        dependency_sensor_code = f"""
    # Wait for previous layer to complete
    wait_for_{previous_layer} = TriggerDagRunOperator(
    task_id="TG_boltpipe_{previous_layer}",
    trigger_dag_id="boltpipe_{previous_layer}",
    conf={{"triggered_by": "boltpipe_{layer}"}},
    wait_for_completion=True,  # wait for it to finish (set False if you don't want to wait)
    )"""
    if layer == list(layer_order)[-1]:
        schedule = "schedule=\"0 5 1 * *\","
    else:
        schedule = "schedule=None,"
    # Write DAG script
    output_file = f"./outputs/airflow/dags/datamart_{layer}.py"
    with open(output_file, 'w', encoding="utf-8") as f:
        f.write(dag_template.format(
            dag_name=f'boltpipe_{layer}',
            operator_imports=runtime_spec["operator_imports"],
            runtime_constants=runtime_spec["runtime_constants"],
            emr_configuration_code=task_operator_code,
            tasks_order=tasks_order,
            dependency_sensor_code=dependency_sensor_code,
            previous_layer=previous_layer,
            run_previous_layer="True" if previous_layer else "False",
            schedule=schedule
        ))
    
    print(
        f"DAG script generated: {output_file} ({len(completed_jobs)} jobs, runtime={runtime_target})"
    )


def process_layer(
    layer: str,
    module_prefix: str,
    jobs: List[Dict],
    config_values: tuple,
    runtime_target: str,
) -> None:
    """Process a single ETL layer and generate all required scripts."""
    print(f"Generating Airflow DAG for layer '{layer}'...")
    
    # Process jobs in dependency order
    completed_jobs = process_job_queue(jobs, layer)
    layer_order = get_layer_order(config_values[-1])
    # Generate individual job scripts
    successful_jobs = set()
    for job in jobs:
        job_name = generate_job_script(job, layer, module_prefix, config_values)
        if job_name:
            successful_jobs.add(job_name)
    
    # Generate main DAG script
    if successful_jobs:
        generate_dag_script(layer, completed_jobs, config_values, layer_order, runtime_target)
    else:
        print(f"Warning: No successful jobs for layer '{layer}', skipping DAG generation")


def create_layer_scripts(config_path: str, target_layers: Optional[List[str]] = None) -> None:
    """
    Main orchestrator that generates separate Python scripts and DAGs for each ETL layer.
    """
    # Load configuration
    config = load_yaml_config(config_path)
    
    # Setup
    create_output_directories()
    config_values = extract_config_values(config)
    layers = filter_layers(config['layers'], target_layers)
    spark_profile = resolve_spark_profile(config_path, config)
    runtime_target = normalize_airflow_runtime(spark_profile.profile)
    print(f"Airflow runtime detected from Spark profile '{spark_profile.profile}': {runtime_target}")
    
    # Process each layer
    for layer, module_prefix in layers.items():
        if layer not in config:
            print(f"Warning: Layer '{layer}' not found in config, skipping...")
            continue
        
        jobs = config.get(layer, [])
        if not jobs:
            print(f"Warning: No jobs found for layer '{layer}', skipping...")
            continue
        
        process_layer(layer, module_prefix, jobs, config_values, runtime_target)


def main() -> None:
    """Main entry point with command line argument handling."""
    target_layers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    if target_layers:
        print(f"Generating scripts for layers: {', '.join(target_layers)}")
    else:
        print("Generating scripts for all layers: flatfile, bronze, silver, gold")
    
    create_layer_scripts("configs/etl_config.yaml", target_layers)
    print("\nLayer script generation completed!")


if __name__ == "__main__":
    main() 
