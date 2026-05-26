from collections import deque
from typing import Dict, List, Optional, Set, Any
import yaml
import sys
import os
import logging

from bolt_pipeliner.generators._paths import ETL_BASE_SOURCE, PACKAGE_ROOT, TEMPLATES_DOCS

# Constants
DEFAULT_CONFIG_PATH = "configs/etl_config.yaml"
TEMPLATE_DIR = str(TEMPLATES_DOCS)
OUTPUTS_DIR = "./outputs"
LAYERS_DIR = "./outputs/layers"
JOB_SCRIPT_TEMPLATE = "job_script.txt"
SPARK_SESSION_FILE = str(PACKAGE_ROOT / "sessions" / "local.py")
ETL_BASE_FILE = str(ETL_BASE_SOURCE)

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load and parse a YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Parsed YAML configuration as a dictionary
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the YAML file is malformed
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {config_path}: {e}")
        raise

def generate_completion_message(layer: str, job_count: int) -> str:
    """
    Generate a completion message for a layer.
    
    Args:
        layer: The layer name (e.g., 'bronze', 'silver')
        job_count: Number of jobs processed in this layer
        
    Returns:
        Formatted completion message string
    """
    return f"""
print("="*60)
print("Completed all {layer.upper()} layer jobs ({job_count} jobs processed)")
print("="*60)
"""

def generate_job_script(
    layer: str, 
    job: Dict[str, Any], 
    job_code: str, 
    module_path: str,
    input_tables: Optional[Dict[str, str]], 
    output_table_name: str,
    partition_by: Optional[str], 
    unload: bool, 
    incremental: bool
) -> str:
    """
    Generate a job script string for the given layer and job definition.
    
    Args:
        layer: The ETL layer name (e.g., 'bronze', 'silver')
        job: Job configuration dictionary
        job_code: The actual Python code for the job
        module_path: Path to the job module file
        input_tables: Dictionary of input table configurations
        output_table_name: Name of the output table
        partition_by: Partition configuration for the output table
        unload: Whether to unload the data
        incremental: Whether this is an incremental job
        
    Returns:
        Formatted job script string
        
    Raises:
        FileNotFoundError: If the job script template is not found
    """
    try:
        with open(f'{TEMPLATE_DIR}/{JOB_SCRIPT_TEMPLATE}', 'r', encoding='utf-8') as f:
            job_script = f.read()
    except FileNotFoundError:
        logger.error(f"Job script template not found: {TEMPLATE_DIR}/{JOB_SCRIPT_TEMPLATE}")
        raise
        
    job_script = job_script.format(
        layer=layer.upper(),
        layer_lower=layer.lower(),
        module=job['module'],
        module_path=module_path,
        unload=unload,
        job_code=job_code,
        output_table_name=output_table_name,
        incremental=incremental,
        partition_by=partition_by,
        input_tables=input_tables
    )
    return job_script

def get_base_script_template() -> str:
    """
    Generate the base script template with Spark session and ETL base code placeholders.
    
    Returns:
        Base script template string with placeholders for ETL base code and bucket configurations
        
    Raises:
        FileNotFoundError: If the Spark session file is not found
    """
    try:
        with open(SPARK_SESSION_FILE, 'r', encoding='utf-8') as f:
            spark_session = f.read()
    except FileNotFoundError:
        logger.error(f"Spark session file not found: {SPARK_SESSION_FILE}")
        raise
        
    # Escape curly braces for string formatting
    spark_session = spark_session.replace('{', '{{').replace('}', '}}')
    
    base_script_template = '''
spark, environment = create_spark_session()
# ETL Base Class
{etl_base_code}

# Bucket Configuration
flatfile_bucket = f"{flatfile_bucket}"
output_bucket = f"{output_bucket}"
'''
    return spark_session + base_script_template


class JobDependencyResolver:
    """
    Handles job dependency resolution and processing order for ETL layers.
    """
    
    def __init__(self, layer: str, jobs: List[Dict[str, Any]]):
        """
        Initialize the job dependency resolver.
        
        Args:
            layer: The ETL layer name
            jobs: List of job configurations
        """
        self.layer = layer
        self.jobs = jobs
        self.processed_jobs: Set[str] = set()
        self.job_queue = deque(jobs)
        
    def _get_job_dependencies(self, job: Dict[str, Any]) -> List[str]:
        """
        Extract input table dependencies from a job configuration.
        
        Args:
            job: Job configuration dictionary
            
        Returns:
            List of input table names that this job depends on
        """
        input_tables = job.get('input_tables', {})
        return [str(table) for table in input_tables.values()]
    
    def _can_process_job(self, job: Dict[str, Any], layer: str, completed_jobs: Set[str]) -> bool:
        """Check if all dependencies for a job are ready."""
        input_tables = [str(v) for v in job.get('input_tables', {}).values()]
        if layer == "flatfile":
            return True
        return all(
            (table in completed_jobs if layer in table else True)
            for table in input_tables
        )
    
    def process_jobs(self) -> List[Dict[str, Any]]:
        """
        Process jobs in dependency order.
        
        Returns:
            List of jobs in processing order
            
        Raises:
            RuntimeError: If circular dependencies are detected
        """
        processed_order = []
        stalled_passes = 0
        max_stalls = len(self.job_queue) + 1  # Safety cap to prevent infinite loops
        
        while self.job_queue and stalled_passes < max_stalls:
            jobs_processed_this_pass = 0
            
            for _ in range(len(self.job_queue)):
                job = self.job_queue.popleft()
                output_table = f"{self.layer}_{job['output_table_name']}"
                if self._can_process_job(job, self.layer, self.processed_jobs):
                    # Mark job as processed
                    self.processed_jobs.add(output_table)
                    processed_order.append(job)
                    jobs_processed_this_pass += 1
                else:
                    # Put job back in queue for next pass
                    self.job_queue.append(job)
            
            if jobs_processed_this_pass == 0:
                stalled_passes += 1
            else:
                stalled_passes = 0  # Reset if we made progress
        
        if self.job_queue:
            remaining_jobs = [job['output_table_name'] for job in self.job_queue]

            raise RuntimeError(
                f"Circular dependencies detected in {self.layer} layer. "
                f"This might be etlconfig missconfiguration."
                f"Remaining jobs: {remaining_jobs}"
            )
        
        return processed_order


def _setup_output_directories() -> None:
    """Create necessary output directories."""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(LAYERS_DIR, exist_ok=True)


def _load_etl_base_code() -> str:
    """
    Load the ETL base code from file.
    
    Returns:
        ETL base code as string
        
    Raises:
        FileNotFoundError: If the ETL base file is not found
    """
    try:
        with open(ETL_BASE_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"ETL base file not found: {ETL_BASE_FILE}")
        raise


def _process_layer_jobs(
    layer: str, 
    jobs: List[Dict[str, Any]], 
    module_prefix: str
) -> tuple[str, int]:
    """
    Process all jobs for a specific layer and generate the job script content.
    
    Args:
        layer: The ETL layer name
        jobs: List of job configurations
        module_prefix: Module path prefix for this layer
        
    Returns:
        Tuple of (job_script_content, job_count)
    """
    job_script_content = f"\n# ============= {layer.upper()} LAYER JOBS =============\n\n"
    job_count = 0
    
    try:
        # Use the dependency resolver to process jobs in correct order
        resolver = JobDependencyResolver(layer, jobs)
        ordered_jobs = resolver.process_jobs()
        
        for job in ordered_jobs:
            module_path = f"./{module_prefix}/{job['module']}.py"
            
            try:
                with open(module_path, 'r', encoding='utf-8') as f:
                    job_code = f.read()
                    
                job_script = generate_job_script(
                    layer=layer,
                    job=job,
                    job_code=job_code,
                    module_path=module_path,
                    input_tables=job.get('input_tables', None),
                    output_table_name=job['output_table_name'],
                    partition_by=job.get('partition_by', None),
                    unload=job.get('unload', True),
                    incremental=job.get('incremental', False)
                )
                job_script_content += job_script
                job_count += 1
                
            except FileNotFoundError:
                logger.warning(f"Could not find module {module_path}, skipping...")
                continue
                
    except RuntimeError as e:
        logger.error(f"Error processing jobs for {layer} layer: {e}")
        raise
    
    return job_script_content, job_count


def _generate_layer_script(
    layer: str, 
    jobs: List[Dict[str, Any]], 
    module_prefix: str,
    etl_base_code: str,
    flatfile_bucket: str,
    output_bucket: str
) -> None:
    """
    Generate a complete layer script file.
    
    Args:
        layer: The ETL layer name
        jobs: List of job configurations for this layer
        module_prefix: Module path prefix for this layer
        etl_base_code: ETL base code content
        flatfile_bucket: Flatfile bucket configuration
        output_bucket: Output bucket configuration
    """
    logger.info(f"Generating {layer}.py...")
    
    # Generate base script template
    script_content = get_base_script_template().format(
        etl_base_code=etl_base_code,
        flatfile_bucket=flatfile_bucket,
        output_bucket=output_bucket
    )
    
    # Add layer-specific jobs
    job_script_content, job_count = _process_layer_jobs(layer, jobs, module_prefix)
    script_content += job_script_content
    
    # Add completion message
    script_content += generate_completion_message(layer, job_count)
    
    # Write layer script to file
    output_file = f"{LAYERS_DIR}/{layer}.py"
    with open(output_file, 'w', encoding="utf-8") as f:
        f.write(script_content)
    
    logger.info(f"Layer script generated: {output_file} ({job_count} jobs)")


def create_layer_scripts(config_path: str, target_layers: Optional[List[str]] = None) -> None:
    """
    Generate separate Python scripts for each ETL layer.
    
    Args:
        config_path: Path to the ETL configuration YAML file
        target_layers: Optional list of specific layers to generate. If None, generates all layers.
        
    Raises:
        FileNotFoundError: If configuration file or required files are not found
        yaml.YAMLError: If configuration file is malformed
        RuntimeError: If job dependencies cannot be resolved
    """
    config = load_yaml_config(config_path)
    
    # Setup directories and load base code
    _setup_output_directories()
    etl_base_code = _load_etl_base_code()
    
    # Extract configuration values
    flatfile_bucket = config['configs']['flatfile_bucket']
    output_bucket = config['configs']['output_bucket']
    layers = config['layers']
    
    # Filter layers if specific ones are requested
    if target_layers:
        layers = {k: v for k, v in layers.items() if k in target_layers}
    
    # Generate scripts for each layer
    for layer, module_prefix in layers.items():
        if layer not in config:
            logger.warning(f"Layer '{layer}' not found in config, skipping...")
            continue
            
        jobs = config.get(layer, [])
        if not jobs:
            logger.warning(f"No jobs found for layer '{layer}', skipping...")
            continue
            
        _generate_layer_script(
            layer=layer,
            jobs=jobs,
            module_prefix=module_prefix,
            etl_base_code=etl_base_code,
            flatfile_bucket=flatfile_bucket,
            output_bucket=output_bucket
        )

def main() -> None:
    """
    Main entry point for the layer script generator.
    """
    # Allow specifying specific layers as command line arguments
    target_layers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    if target_layers:
        logger.info(f"Generating scripts for layers: {', '.join(target_layers)}")
    else:
        logger.info("Generating scripts for all layers: flatfile, bronze, silver, gold")
    
    try:
        create_layer_scripts(DEFAULT_CONFIG_PATH, target_layers)
        logger.info("Layer script generation completed successfully!")
    except Exception as e:
        logger.error(f"Layer script generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 