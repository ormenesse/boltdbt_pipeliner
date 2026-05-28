from nbformat.v4 import new_notebook, new_code_cell
from collections import deque
import nbformat
import yaml
import sys
import os

from bolt_pipeliner.config import resolve_data_locations
from bolt_pipeliner.generators._paths import ETL_BASE_SOURCE, PACKAGE_ROOT, TEMPLATES_DOCS
from bolt_pipeliner.sessions.profiles import resolve_spark_profile


def load_yaml_config(path):
    """Load YAML configuration from file."""
    with open(path, 'r', encoding="utf-8") as f:
        return yaml.safe_load(f)


def add_initial_cells(notebook, config, config_path):
    """Add initial setup cells to the notebook."""
    # CSS styling cell
    css_style = """%%html
<style>
div.jp-OutputArea-output pre {
    white-space: pre;
}
</style>"""
    notebook.cells.append(new_code_cell(css_style))
    
    # Spark configuration
    spark_profile = resolve_spark_profile(config_path, config)
    spark_config_source = str(spark_profile.path) if spark_profile.path else "defaults"
    notebook.cells.append(new_code_cell(
        "# Spark configuration resolved from etl_config.yaml / configs/spark/*.toml\n"
        f"spark_profile = {spark_profile.profile!r}\n"
        f"spark_config_source = {spark_config_source!r}\n"
        f"spark_config = {spark_profile.spark_config!r}"
    ))
    
    # Spark session setup
    with open(str(PACKAGE_ROOT / "sessions" / "local.py"), "r", encoding="utf-8") as f:
        spark_session_code = f.read()
    notebook.cells.append(new_code_cell(spark_session_code))
    notebook.cells.append(new_code_cell("spark = create_session(spark_config=spark_config)"))

    # ETL base code
    with open(str(ETL_BASE_SOURCE), 'r', encoding="utf-8") as f:
        etl_base_code = f.read()
    notebook.cells.append(new_code_cell(source=f"# etl_base.py\n{etl_base_code}"))
    
    # Bucket configuration
    flatfile_location, output_location = resolve_data_locations(config)
    bucket_config = f'''flatfile_bucket = f"{flatfile_location}"
output_bucket = f"{output_location}"'''
    notebook.cells.append(new_code_cell(bucket_config))


def filter_layers(config, layers_arg):
    """Filter layers based on command line arguments."""
    layers = config['layers']
    
    if layers_arg:
        # Filter to only specified layers
        filtered_layers = {k: v for k, v in layers.items() if k in layers_arg}
        return filtered_layers
    else:
        return layers


def get_job_dependencies(job):
    """Extract input table dependencies from job configuration."""
    return [str(v) for v in job.get('input_tables', {}).values()]


def has_unmet_dependencies(job, layer, completed_jobs):
    """Check if job has unmet dependencies."""
    dependencies = get_job_dependencies(job)
    return any((layer in table) and (table not in completed_jobs) for table in dependencies)


def process_job_queue(layer, jobs, module_prefix, notebook):
    """Process jobs in dependency order and add them to notebook."""
    queue = deque(jobs)
    completed_jobs = set()
    stalled_passes = 0
    max_stalls = len(queue) + 1  # Safety cap to prevent infinite loops
    
    while queue and stalled_passes < max_stalls:
        jobs_processed_this_pass = 0
        
        for _ in range(len(queue)):
            job = queue.popleft()
            
            # Check if job can be processed (no unmet dependencies)
            if has_unmet_dependencies(job, layer, completed_jobs):
                queue.append(job)  # Put back in queue
            else:
                # Process the job
                job_script_name = f"{layer}_{job['output_table_name']}"
                completed_jobs.add(job_script_name)
                jobs_processed_this_pass += 1
                
                add_job_to_notebook(job, layer, module_prefix, notebook)
        
        # Check if we made progress
        if jobs_processed_this_pass == 0:
            stalled_passes += 1
        else:
            stalled_passes = 0


def add_job_to_notebook(job, layer, module_prefix, notebook):
    """Add a single job to the notebook."""
    # Load job template
    with open(str(TEMPLATES_DOCS / "job_script.txt"), "r", encoding='utf-8') as f:
        job_template = f.read()
    
    # Load job module code
    module_path = f"./{module_prefix}/{job['module']}.py"
    with open(module_path, 'r') as f:
        job_code = f.read()
    
    # Format the job script
    job_script = job_template.format(
        layer=layer.upper(),
        layer_lower=layer.lower(),
        module=job['module'],
        job=job,
        job_code=job_code,
        module_path=module_path,
        etl_base_code="",
        input_tables=job.get('input_tables', None),
        output_table_name=job['output_table_name'],
        partition_by=job.get('partition_by', None),
        unload=job.get('unload', True),
        incremental=job.get('incremental', False)
    )
    
    # Add to notebook
    notebook.cells.append(new_code_cell(source=job_script))
    notebook.cells.append(new_code_cell(source="spark.catalog.clearCache()"))


def create_etl_notebook(config_path, layers_arg=None, output_file="etl_jobs_notebook.ipynb"):
    """
    Generate ETL pipeline notebook for Spark EMR.
    
    Args:
        config_path: Path to YAML configuration file
        layers_arg: List of specific layers to process (optional)
        output_file: Output notebook filename
    """
    # Load configuration
    config = load_yaml_config(config_path)
    
    # Filter layers if specified
    layers_to_process = filter_layers(config, layers_arg)
    
    # Skip if only one layer (seems to be a special case in original code)
    if len(layers_to_process) == 1:
        layers_to_process = {}
    
    # Create notebook
    notebook = new_notebook()
    
    # Add initial setup cells
    add_initial_cells(notebook, config, config_path)
    
    # Process each layer
    for layer, module_prefix in layers_to_process.items():
        if layer not in config:
            print(f"Warning: Layer '{layer}' not found in config, skipping...")
            continue
        
        jobs = config.get(layer, [])
        process_job_queue(layer, jobs, module_prefix, notebook)
    
    # Write notebook to file
    output_path = f"./outputs/notebook/{output_file}"
    os.makedirs("./outputs/notebook", exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        nbformat.write(notebook, f)
    
    print(f"Notebook generated: {output_file}")


if __name__ == "__main__":
    # Run this to generate the notebook
    create_etl_notebook("configs/etl_config.yaml", layers_arg=sys.argv)
