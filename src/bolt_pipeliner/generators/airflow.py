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
    'container_operator': str(TEMPLATES_AIRFLOW / "emrcontaineroperator.txt"),
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
    return (
        configs['flatfile_bucket'],
        configs['output_bucket'], 
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


def generate_dag_script(layer: str, completed_jobs: List[str], 
                       config_values: tuple, layer_order: List[str]) -> None:
    """Generate the main DAG script for a layer with dependencies."""
    _, _, fixed_schema, _ = config_values
    
    # Read DAG templates
    dag_template = read_template_file(TEMPLATE_FILES['dag_template'])
    container_op = read_template_file(TEMPLATE_FILES['container_operator'])
    
    if not dag_template or not container_op:
        print(f"Error: Could not read DAG templates for layer '{layer}'")
        return
    
    # Build EMR configuration and task order
    emr_configuration_code = ""
    tasks_order = ""
    
    for job_name in completed_jobs:
        emr_configuration_code += "        " + container_op.format(
            job_script_name=job_name,
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
    #     external_dag_id="etrdatamart_{previous_layer}",
    #     external_task_id="TG_etrdatamart_{previous_layer}",
    #     timeout=3600*12,  # 12 hour timeout
    #     poke_interval=60,  # Check every minute
    #     mode="reschedule",
    #     allowed_states=[DagRunState.SUCCESS],
    # )"""
        dependency_sensor_code = f"""
    # Wait for previous layer to complete
    wait_for_{previous_layer} = TriggerDagRunOperator(
    task_id="TG_etrdatamart_{previous_layer}",
    trigger_dag_id="etrdatamart_{previous_layer}",
    conf={{"triggered_by": "etrdatamart_{layer}"}},
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
            dag_name=f'etrdatamart_{layer}',
            emr_configuration_code=emr_configuration_code,
            tasks_order=tasks_order,
            dependency_sensor_code=dependency_sensor_code,
            previous_layer=previous_layer,
            run_previous_layer="True" if previous_layer else "False",
            schedule=schedule
        ))
    
    print(f"DAG script generated: {output_file} ({len(completed_jobs)} jobs)")


def process_layer(layer: str, module_prefix: str, jobs: List[Dict], 
                 config_values: tuple) -> None:
    """Process a single ETL layer and generate all required scripts."""
    print(f"Generating Airflow DAG for layer '{layer}'...")
    
    # Process jobs in dependency order
    completed_jobs = process_job_queue(jobs, layer)
    layer_oder = config_values[-1].keys()
    # Generate individual job scripts
    successful_jobs = set()
    for job in jobs:
        job_name = generate_job_script(job, layer, module_prefix, config_values)
        if job_name:
            successful_jobs.add(job_name)
    
    # Generate main DAG script
    if successful_jobs:
        generate_dag_script(layer, completed_jobs, config_values, layer_oder)
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
    
    # Process each layer
    for layer, module_prefix in layers.items():
        if layer not in config:
            print(f"Warning: Layer '{layer}' not found in config, skipping...")
            continue
        
        jobs = config.get(layer, [])
        if not jobs:
            print(f"Warning: No jobs found for layer '{layer}', skipping...")
            continue
        
        process_layer(layer, module_prefix, jobs, config_values)


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