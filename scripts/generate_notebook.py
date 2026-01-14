from nbformat.v4 import new_notebook, new_code_cell
from collections import deque
import nbformat
import yaml
import sys
import os


def load_yaml_config(path):
    """Load YAML configuration from file."""
    with open(path, 'r', encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_spark_config_cell():
    """Create the Spark configuration cell with proper formatting."""
    spark_config = {
        "driverMemory": "27G",
        "executorMemory": "27G", 
        "executorCores": 4,
        "driverCores": 4,
        "conf": {
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            "spark.dynamicAllocation.minExecutors": 1,
            "spark.dynamicAllocation.maxExecutors": 20,
            "spark.dynamicAllocation.initialExecutors": 2,
            "spark.sql.shuffle.partitions": 1000,
            "spark.sql.adaptive.shuffle.partitions": "true",
            "spark.kubernetes.executor.label.pod_name": "utility",
            "spark.kubernetes.driver.label.pod_name": "utility",
            "spark.kubernetes.executor.label.product": "nps-sensitivy",
            "spark.kubernetes.driver.label.product": "nps-sensitivy",
            "spark.kubernetes.executor.label.developer": "vormene",
            "spark.kubernetes.driver.label.developer": "vormene",
            "spark.kubernetes.exector.label.domain": "emrnotebook",
            "spark.kubernetes.driver.label.domain": "emrnotebook",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.local.dir": "/tmp/spark-temp",
            "spark.hadoop.fs.s3a.committer.name": "directory",
            "spark.hadoop.fs.s3a.committer.staging.tmp.path": "/tmp/spark-s3a-staging",
            "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false"
        }
    }
    
    config_str = f"%%configure -f\n{spark_config}"
    return new_code_cell(config_str)


def add_initial_cells(notebook, config):
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
    notebook.cells.append(create_spark_config_cell())
    
    # Spark session setup
    with open("./spark_session.py", "r", encoding="utf-8") as f:
        spark_session_code = f.read()
    notebook.cells.append(new_code_cell(spark_session_code))
    notebook.cells.append(new_code_cell("spark = create_spark_session()"))
    
    # ETL base code
    with open('./etl_base.py', 'r', encoding="utf-8") as f:
        etl_base_code = f.read()
    notebook.cells.append(new_code_cell(source=f"# etl_base.py\n{etl_base_code}"))
    
    # Bucket configuration
    bucket_config = f'''flatfile_bucket = f"{config["configs"]["flatfile_bucket"]}"
output_bucket = f"{config["configs"]["output_bucket"]}"'''
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
    with open("./template/job_script.txt", "r", encoding='utf-8') as f:
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
        layers_to_process = []
    
    # Create notebook
    notebook = new_notebook()
    
    # Add initial setup cells
    add_initial_cells(notebook, config)
    
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
