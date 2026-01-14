
# BOLTDBT PIPELINER - ETL Pipeline Documentation

  

## Overview

  

The BOLTDBT PIPELINER is a comprehensive ETL (Extract, Transform, Load) pipeline built on Apache Spark with Iceberg tables, designed to process customer data for Entergy. The system follows a medallion architecture (Bronze → Silver → Domain layers) and provides automated code generation, documentation, and Airflow DAG creation.

  

## Table of Contents

  
1. [Core Components](#core-components)

2. [ETL Configuration](#etl-configuration)

3. [ETL Folder Structure](#etl-folder-structure)

4. [Code Generation System](#code-generation-system)

5. [Getting Started Guide](#getting-started-guide)

6. [Adding New ETL Jobs](#adding-new-etl-jobs)

  

---

  

## Core Components

  

### 1. main.py

  

**Purpose**: The main entry point for running ETL jobs across different layers.

  

**Key Features**:

-  **Command-line interface** with arguments for different ETL layers (`--flatfile`, `--bronze`, `--silver`, `--domain`)

-  **Dynamic job execution** based on YAML configuration

-  **Environment detection** (dev/test/prod) through AWS account ID

-  **Modular execution** allowing you to run specific layers or all layers

  

**Usage Examples**:

```bash

# Run all layers

python  main.py

  

# Run only bronze layer

python  main.py  --bronze

  

# Run multiple layers

python  main.py  --bronze  --silver

  

# Use custom config

python  main.py  --config  custom_config.yaml  --silver

```

  

**How it works**:

1. Loads the ETL configuration from `etl_config.yaml`

2. Creates a Spark session using `spark_session.py`

3. Iterates through requested layers and executes jobs defined in the config

4. For each job, dynamically imports the corresponding module and attaches the `process_data` method to the ETLBase class

5. Executes the ETL pipeline with proper error handling and logging

  

### 2. etl_config.yaml

  

**Purpose**: Central configuration file that defines all ETL jobs, their dependencies, and execution parameters.

  

**Structure**:

```yaml

configs:

output_bucket: "s3:/..."

flatfile_bucket: "s3://.."

schema: datamart

catalog: dev_catalog

  

layers:

flatfile: etl/_flatfile

bronze: etl/0_bronze

silver: etl/1_silver

domain: etl/2_domain

  

# Job definitions for each layer

flatfile: []

bronze: []

silver: []

domain: []

```

  

**Key Configuration Elements**:

  

-  **`configs`**: Global settings including S3 buckets, schema names, and catalog configurations

-  **`layers`**: Defines the folder structure for each ETL layer

-  **Job definitions**: Each job includes:

-  `module`: Python file name (without .py extension)

-  `description`: Human-readable description

-  `input_tables`: Dictionary mapping aliases to source tables/files

-  `output_table_name`: Target table name

-  `partition_by`: List of columns for partitioning

-  `incremental`: Boolean for incremental processing

-  `unload`: Boolean for whether to save table results

  

**Example Job Configuration**:

```yaml

-  module: silver_fct_account_calls_monthly

description: "Aggregated call count and hold time metrics at account-month grain"

incremental: true

class_name: ETLBase

input_tables:

t_agent_calls: bronze_t_agent_calls

output_table_name: fct_account_calls_monthly

partition_by:

-  year_month

```

  

### ETLBase Class Integration

  

**Purpose**: The `ETLBase` class in `etl_base.py` is the core engine that processes all ETL jobs defined in `etl_config.yaml`. It provides a standardized framework for data loading, processing, and unloading across all layers.
This class can be configured and changed with the company demands, it's framework agnostic. In this README, it is presented as the best solution for ENTERGY datamart which has, in it's core, many time series tables.

  

#### **How ETLBase Works with Configuration**

  

**1. Table Naming Convention**:

The ETLBase class automatically constructs the final table name by combining the layer and output_table_name:

```python

# From etl_base.py line 47

self.iceberg_table =  f"{self.save_catalog}.{self.FIXED_SCHEMA}.{self.layer}_{self.output_table_name}"

```

  

**Example**:

- Layer: `silver`

- Output table name: `fct_account_calls_monthly`

- Final table: `dev_catalog.etrdatamart.silver_fct_account_calls_monthly`

  

**2. Configuration Parameter Mapping**:

  

| Config Parameter | ETLBase Property | Purpose |

|------------------|------------------|---------|

| `incremental` | `self.incremental` | Controls incremental vs full refresh processing |

| `partition_by` | `self.partition_by` | Defines table partitioning strategy |

| `unload` | `self.unload` | Determines whether to save results to Iceberg |

| `input_tables` | `self.input_table_names` | Maps input table aliases to source tables |

  

#### **Incremental Processing**

  

**How it works**:

```python

# From etl_base.py lines 68-89

def  check_if_tables_exists_find_yearmonths(self):

if  not  self.incremental:

self.year_months = None

return

# If incremental and table exists, process last 3 months

if  self._table_exists(self.iceberg_table):

# Calculate last 3 months + current month

self.year_months =  [calculated_month_sequence]

else:

self.year_months = None # Full refresh for new tables

```

  

**Configuration Example**:

```yaml

-  module: silver_fct_account_calls_monthly

incremental: true  # Enables incremental processing

partition_by:

-  year_month  # Required for incremental processing

```

  

**Behavior**:

-  **First run**: Processes all historical data (full refresh)

-  **Subsequent runs**: Only processes last 3 months of data

-  **Performance benefit**: Significantly faster for large historical datasets

  

#### **Table Existence and Creation**

  

**Automatic Table Management**:

```python

# From etl_base.py lines 154-166

if  not  self.table_exists:

# First write → CREATE TABLE

self._create_table(processed_df)

else:

# Table exists → UPDATE PARTITIONS

if  self.incremental and  self.year_months is  not None:

target_df = processed_df.filter(F.col("year_month").isin(self.year_months))

self._replace_table_partitions(target_df)

else:

# Full refresh

self._replace_table_partitions(processed_df)

```

  

**Key Features**:

-  **Automatic schema creation**: Creates Iceberg tables on first run

-  **Partition management**: Handles partition creation and updates

-  **Schema evolution**: Supports adding new columns automatically

  

#### **Advanced Partition Unloading Pattern**

  

**Performance Optimization**: For large datasets, you can implement custom partition unloading within the `process_data` function to improve performance and memory management.

  

**Example from `silver_fct_account_invoice_details_monthly.py`**:

```python

def  process_data(self, input_tables):

# ... data processing logic ...

# Process data month by month for performance

for ym in month_seq:

# Filter data for current month

to_write = pivot_df.filter(F.col("year_month")  == ym)

# Write partition incrementally

if  not  self.table_exists:

self._create_table(to_write)

else:

self._replace_table_partitions(to_write)

# Clean up memory

if  'df'  in  locals():

df.unpersist()

if  'pivot_df'  in  locals():

pivot_df.unpersist()

# Return empty DataFrame to skip ETLBase unload

return  self.spark.createDataFrame([],  schema='contract_account_id INT, year_month INT')

```

  

**Benefits of Custom Partition Unloading**:

1.  **Memory efficiency**: Processes data in smaller chunks

2.  **Performance**: Avoids memory overflow on large datasets

3.  **Fault tolerance**: If job fails, completed partitions are preserved

4.  **Parallel processing**: Can be extended for parallel partition processing

  

**Configuration for Custom Unloading**:

```yaml

-  module: silver_fct_account_invoice_details_monthly

unload: false  # Disable ETLBase unload since we handle it manually

incremental: true

partition_by:

-  year_month

```

  

#### **Data Loading Behavior by Layer**

  

**Flatfile Layer**:

```python

# From etl_base.py lines 105-113

if  self.layer ==  "flatfile":

for key, rel_path in  self.input_table_names.items():

self.input_tables[key]  =  self.spark.read.csv(

f"{self.bucket}/{rel_path}",

header=True,  inferSchema=True,  multiLine=True,

escape='"',  quote='"'

)

```

  

**Bronze Layer**:

```python

# From etl_base.py lines 115-129

if  self.layer ==  "bronze":

for key in  self.input_table_names.keys():

if  '.'  in  self.input_table_names[key]:

# External catalog reference

self.input_tables[key]  =  self.spark.sql(

f"SELECT * FROM {self.catalog}.{self.input_table_names[key]}"

)

else:

# Internal table reference

table_ident =  f"{self.save_catalog}.{self.FIXED_SCHEMA}.{self.input_table_names[key]}"

self.input_tables[key]  =  self.spark.read.table(table_ident)

```

  

**Silver/Domain Layers**:

```python

# From etl_base.py lines 131-135

for key, name in  self.input_table_names.items():

table_ident =  f"{self.save_catalog}.{self.FIXED_SCHEMA}.{name}"

self.input_tables[key]  =  self.spark.read.table(table_ident)

```

  

#### **Configuration Best Practices**

  

**1. Incremental Processing Setup**:

```yaml

-  module: silver_fct_account_calls_monthly

incremental: true

partition_by:

-  year_month  # Required for incremental processing

unload: true

```

  

**2. Full Refresh Jobs**:

```yaml

-  module: silver_dim_customer_demographic

incremental: false  # Always process all data

unload: true

```

  

**3. Custom Partition Unloading**:

```yaml

-  module: silver_fct_large_dataset

incremental: true

partition_by:

-  year_month

unload: false  # Handle unloading manually in process_data

```

  

**4. Memory-Intensive Jobs**:

```yaml

-  module: silver_fct_complex_aggregation

incremental: true

partition_by:

-  year_month

-  account_type  # Multiple partitions for better performance

unload: true

```

  

#### **Error Handling and Logging**

  

**Built-in Logging**:

```python

# ETLBase provides consistent logging across all jobs

self.logging_string =  f"{self.layer}  {self.output_table_name}"

logging.info(f"{self.logging_string} - Loading data...")

logging.info(f"{self.logging_string} - Processing data...")

logging.info(f"{self.logging_string} - Saving data...")

```

  

**Configuration for Debugging**:

```yaml

-  module: silver_fct_debug_job

description: "Debug job with detailed logging"

incremental: false  # Use full refresh for debugging

unload: true

```

  

This integration between `etl_config.yaml` and `ETLBase` provides a powerful, standardized framework for ETL processing with automatic optimization, error handling, and performance tuning capabilities.

  

### 3. ETL Folder Structure

  

The `etl/` folder contains the actual ETL job implementations organized by layers:

  

#### **`etl/_flatfile/`** - Raw Data Ingestion

-  **Purpose**: Processes CSV files and external data sources

-  **Data Sources**: Storm events, NPS surveys, gas prices, CPI data, zip codes

-  **Processing**: Minimal transformation, mainly data cleaning and standardization

-  **Output**: Iceberg tables in the flatfile layer

  

**Example**: `flatfile_storm_events.py`

```python

def  process_data(self, input_tables):

logging.info(f"{self.logging_string} - Initializing processing...")

result_df = input_tables['storm_events'].select(

input_tables['storm_events'].columns

)

return result_df

```

  

#### **`etl/0_bronze/`** - Raw Data Storage

-  **Purpose**: Ingests data from various source systems (Salesforce, CCS, ADMS, etc.)

-  **Data Sources**: Customer data warehouse, contact center, weather systems

-  **Processing**: Basic data type conversions and column selection

-  **Output**: Standardized raw data in Iceberg format

  

**Example**: `bronze_account.py`

```python

def  process_data(self, input_tables):

result_df = input_tables['account'].select([

'id',  'name',  'type',  'billingstreet',  'billingcity',

# ... other columns

])

return result_df

```

  

#### **`etl/1_silver/`** - Business Logic Layer

-  **Purpose**: Applies business rules, aggregations, and transformations

-  **Processing**: Complex joins, calculations, time-based aggregations

-  **Features**: Rolling windows, year-over-year comparisons, customer metrics

-  **Output**: Business-ready datasets for analytics

  

**Example**: `silver_fct_account_calls_monthly.py`

```python

def  process_data(self, input_tables):

# Complex aggregation logic with rolling windows

# Customer call metrics by month

# 3-month, 6-month, 12-month rolling averages

return processed_df

```

  

#### **`etl/2_domain/`** - Domain-Specific Aggregations

-  **Purpose**: Creates domain-specific fact tables for specific business areas

-  **Processing**: Combines multiple silver tables into comprehensive views

-  **Examples**: Customer experience, financial metrics, operational data

-  **Output**: Final business intelligence datasets

  

### 4. spark_session.py

  

**Purpose**: Configures and creates the Apache Spark session with all necessary settings for the BOLTDBT PIPELINER environment.

  

**Key Features**:

  

#### **Environment Detection**:

```python

environment = {

'942249008577': 'prod',

'783574663203': 'test',

'308359645995': 'dev'

}.get(account_id)

```

  

#### **Spark Configuration**:

-  **Memory settings**: 27GB driver/executor memory, 4 cores each

-  **Dynamic allocation**: 1-20 executors based on workload

-  **Performance tuning**: Adaptive query execution, shuffle optimization

-  **S3 integration**: Fast upload, multi-object delete settings

  

#### **Iceberg + Glue Catalog Setup**:

```python

# Dev catalog for BOLTDBT PIPELINER

.config("spark.sql.catalog.dev_catalog",  "org.apache.iceberg.spark.SparkCatalog")

.config("spark.sql.catalog.dev_catalog.catalog-impl",  "org.apache.iceberg.aws.glue.GlueCatalog")

  

# Shared catalog for source data

.config("spark.sql.catalog.shared_catalog",  "org.apache.iceberg.spark.SparkCatalog")

```

  

#### **Kubernetes Labels**:

- Product: nps-sensitivity

- Developer: vormene

- Domain: emrnotebook

  

---

  

## Code Generation System

  

### generate.py

  

**Purpose**: Main orchestrator for all code generation tasks. This script intelligently analyzes your ETL configuration and generates all necessary artifacts for deployment, documentation, and orchestration.

  

**Available Options**:

```bash

python  generate.py  airflow  # Generate Airflow DAGs

python  generate.py  documentation  # Generate HTML documentation

python  generate.py  layers  # Generate ETL layer scripts

python  generate.py  notebook  # Generate Jupyter notebook

python  generate.py  snowflakeddl  # Generate Snowflake DDLs

python  generate.py  all  # Generate everything

```

  

**Smart Dependency Resolution**: The generation system is intelligent enough to automatically determine the correct execution order of your ETL jobs, regardless of how you order them in `etl_config.yaml`. It analyzes the `input_tables` dependencies and creates a proper DAG (Directed Acyclic Graph) for execution.

  

### Detailed Option Explanations

  

#### **1. Airflow DAG Generation (`python generate.py airflow`)**

  

**What it does**:

- Analyzes all ETL jobs in your configuration

- Creates dependency graphs based on `input_tables` relationships

- Generates Airflow DAGs with proper task dependencies

- Configures EMR container operators for Spark job execution

  

**Output Location**: `outputs/airflow/`

  

**Generated Files**:

-  **`dags/etr_datamart_dag.py`**: Main Airflow DAG file

-  **`dags/etr_datamart_flatfile_dag.py`**: Flatfile-specific DAG

-  **`dags/etr_datamart_bronze_dag.py`**: Bronze layer DAG

-  **`dags/etr_datamart_silver_dag.py`**: Silver layer DAG

-  **`dags/etr_datamart_domain_dag.py`**: Domain layer DAG

-  **`code/`**: Individual EMR container operator scripts for each ETL job

  

**Key Features**:

-  **Automatic dependency resolution**: Jobs are ordered based on their input dependencies

-  **EMR integration**: Uses EMR container operators for Spark execution

-  **Error handling**: Built-in retry logic and failure notifications

-  **Resource management**: Configurable Spark parameters per job

-  **Monitoring**: Task success/failure tracking

  

**Coding Rules**:

- Job order in `etl_config.yaml` doesn't matter - dependencies are auto-resolved

- Each job becomes an Airflow task with proper upstream/downstream relationships

- Failed jobs automatically retry with exponential backoff

- Jobs can be run independently or as part of the full pipeline

  

**Deployment Instructions**:

1.  **Upload DAG files** to:

```

s3://entergy-bdi-dataeng-code-repo-dev/mwaa/dags/entergy-bdi-etl/etrdatamart/

```

2.  **Upload EMR container operator scripts** to:

```

s3://entergy-bdi-dataeng-code-repo-dev/entergy-bdi-etl/etl_framework/scripts/etrdatamart/

```

3.  **Go to your airflow and check wether the Dags are in the system or not.**
  

#### **2. Documentation Generation (`python generate.py documentation`)**

  

**What it does**:

- Creates comprehensive HTML documentation for your entire ETL pipeline

- Generates data lineage diagrams using Mermaid

- Creates interactive table schemas and descriptions

- Builds dependency visualization graphs

  

**Output Location**: `outputs/documentation/`

  

**Generated Files**:

-  **`index.html`**: Main documentation homepage

-  **`tables/`**: Individual table documentation pages

-  **`logo.png`**: Project branding (copied from `template/logo.png`)

-  **Interactive features**: Clickable lineage diagrams, searchable tables

  

**Prerequisites**:

- Ensure `logo.png` exists in the `template/` folder before generating documentation

- The logo will be automatically copied to the documentation output

  

**Key Features**:

-  **Data lineage visualization**: Shows how data flows from source to final tables

-  **Table documentation**: Auto-generated from job descriptions and schemas

-  **Interactive diagrams**: Mermaid-based dependency graphs

-  **Responsive design**: Works on desktop and mobile devices

-  **Search functionality**: Find tables and jobs quickly

  

**Styling**: Uses `style_config.yaml` for consistent branding and colors across all documentation.

  

#### **3. ETL Layer Scripts (`python generate.py layers`)**

  

**What it does**:

- Creates standalone, executable Python scripts for each ETL layer

- Generates self-contained scripts with all necessary imports and configurations

- Creates scripts that can run independently of the main pipeline

  

**Output Location**: `outputs/layers/`

  

**Generated Files**:

-  **`bronze.py`**: Executable script for all bronze layer jobs

-  **`silver.py`**: Executable script for all silver layer jobs

-  **`domain.py`**: Executable script for all domain layer jobs

-  **`flatfile.py`**: Executable script for all flatfile jobs

  

**Key Features**:

-  **Self-contained**: Each script includes Spark session creation and configuration

-  **Dependency-aware**: Jobs execute in the correct order based on dependencies

-  **Error handling**: Comprehensive logging and error reporting

-  **Modular execution**: Can run individual layers or the entire pipeline

  

**Usage**:

```bash

# Run bronze layer only

python  outputs/layers/bronze.py

# Run silver layer only

python  outputs/layers/silver.py

```

  

#### **4. Jupyter Notebook Generation (`python generate.py notebook`)**

  

**What it does**:

- Creates a comprehensive Jupyter notebook for interactive development and testing

- Generates cells for each ETL job with example code

- Provides data exploration and visualization capabilities

- Includes sample queries and data quality checks

  

**Output Location**: `outputs/notebook/`

  

**Generated Files**:

-  **`etl_jobs_notebook.ipynb`**: Complete Jupyter notebook

  

**Key Features**:

-  **Interactive development**: Test individual transformations before deployment

-  **Data exploration**: Built-in data quality checks and sample queries

-  **Visualization**: Matplotlib/Plotly integration for data visualization

-  **Documentation**: Markdown cells explaining each transformation

-  **Reproducible**: Can be used for data science and analytics work

  

**Use Cases**:

- Development and testing of new ETL jobs

- Data exploration and analysis

- Performance tuning and optimization

- Data quality assessment

  

#### **5. Snowflake DDL Generation (`python generate.py snowflakeddl`)**

  

**What it does**:

- Generates Snowflake DDL (Data Definition Language) statements

- Creates table creation scripts based on your ETL job configurations

- Generates schema definitions and partition specifications

- Creates scripts for setting up Snowflake tables

  

**Output Location**: `outputs/snowflake_ddls/`

  

**Generated Files**:

-  **`snowflake_ddl.sql`**: Complete DDL script for all tables

-  **`schema.py`**: Python schema definitions

-  **`schema.csv`**: Tabular schema information

  

**Key Features**:

-  **Table creation**: DDL statements for all ETL output tables

-  **Schema definitions**: Column types, constraints, and descriptions

-  **Partition specifications**: Based on your `partition_by` configurations

-  **Cross-platform compatibility**: Works with Snowflake data warehouse

  

**Use Cases**:

- Setting up Snowflake tables for data warehouse

- Schema migration and version control

- Documentation of table structures

- Cross-platform data architecture

  

### Smart Dependency Resolution

  

**How it works**:

1.  **Dependency Analysis**: The system reads your `etl_config.yaml` and analyzes the `input_tables` for each job

2.  **Graph Construction**: Creates a dependency graph where each job is a node and dependencies are edges

3.  **Topological Sorting**: Uses graph algorithms to determine the correct execution order

4.  **Cycle Detection**: Prevents circular dependencies that would cause infinite loops

5.  **Layer Grouping**: Groups jobs by layer while maintaining proper dependencies

  

**Example**:

```yaml

# Order in config doesn't matter - system will auto-resolve

silver:
  -  module: silver_job_c
    input_tables:
      source: bronze_job_a  # Depends on bronze_job_a
  -  module: silver_job_a
    input_tables:
      source: bronze_job_b  # Depends on bronze_job_b
  -  module: silver_job_b
    input_tables:
      source: bronze_job_a  # Depends on bronze_job_a
```

  

**Execution Order** (automatically determined):

1.  `bronze_job_a` (no dependencies)

2.  `bronze_job_b` (depends on bronze_job_a)

3.  `silver_job_a` (depends on bronze_job_b)

4.  `silver_job_c` (depends on bronze_job_a)

  

### Coding Rules and Best Practices

  

#### **ETL Configuration Rules**:

1.  **Module naming**: Must match the Python file name (without .py extension)

2.  **Input table aliases**: Use descriptive names that match your processing logic

3.  **Output table naming**: Follow the pattern `{layer}_{table_name}`

4.  **Dependency clarity**: Make input dependencies explicit and clear

5.  **Description quality**: Write detailed descriptions for documentation generation

  

#### **Job Implementation Rules**:

1.  **Function signature**: Always use `def process_data(self, input_tables):`

2.  **Return DataFrame**: Always return a PySpark DataFrame

3.  **Logging**: Use `self.logging_string` for consistent log messages

4.  **Error handling**: Include proper exception handling

5.  **Data validation**: Add data quality checks where appropriate

  

#### **Performance Considerations**:

1.  **Partitioning**: Use `partition_by` for large tables

2.  **Incremental processing**: Enable `incremental: true` for time-series data

3.  **Resource optimization**: Consider Spark memory and executor settings

4.  **Data types**: Use appropriate data types to minimize memory usage

  

### style_config.yaml

  

**Purpose**: Defines the visual styling for generated documentation and maintains consistent branding across all generated artifacts.

  

**Key Sections**:

-  **Layer colors**: Different colors for each ETL layer (bronze, silver, domain)

-  **UI elements**: Backgrounds, borders, shadows for documentation panels

-  **Typography**: Text colors, headings, links

-  **Mermaid diagrams**: Colors for data lineage visualization

  

**Customization**: You can modify colors, fonts, and styling to match your organization's branding guidelines.

  

---

  

## Getting Started Guide

  

### Prerequisites

  

1.  **Python Environment**: Python 3.8+ with required packages

2.  **AWS Credentials**: Configured for the appropriate environment (dev/test/prod)

3.  **Spark Environment**: Access to EMR or Spark cluster

4.  **S3 Access**: Permissions to read/write to BOLTDBT PIPELINER buckets

  

### Initial Setup

  

1.  **Clone the repository**:

```bash

git clone <repository-url>

cd etr_datamart

```

  

2.  **Install dependencies**:

```bash

pip install -r requirements.txt

```

  

3.  **Configure AWS credentials**:

```bash

aws configure

# or set environment variables

export  AWS_ACCESS_KEY_ID=your_key

export  AWS_SECRET_ACCESS_KEY=your_secret

```

  

4.  **Generate all artifacts**:

```bash

python generate.py all

```

  

### Running ETL Jobs

  

1.  **Run all layers**:

```bash

python main.py

```

  

2.  **Run specific layer**:

```bash

python main.py --bronze

python main.py --silver

python main.py --domain

```

  

3.  **Run with custom config**:

```bash

python main.py --config custom_config.yaml --silver

```

  

---

  

## Adding New ETL Jobs

  

### Step 1: Create the ETL Module

  

1.  **Navigate to the appropriate layer folder**:

```bash

cd etl/1_silver/ # or appropriate layer

```

  

2.  **Create a new Python file**:

```python

# silver_fct_new_metric.py

import logging

from pyspark.sql import functions as F

def  process_data(self, input_tables):

  logging.info(f"{self.logging_string} - Initializing processing...")
  # Your transformation logic here
  result_df = input_tables['source_table'].select(
    F.col('account_id'),
    F.col('metric_value'),
    F.current_timestamp().alias('processed_at')
  )
  return result_df

```

  

### Step 2: Update etl_config.yaml

  

Add your job configuration to the appropriate layer section:

  

```yaml

silver:

-  module: silver_fct_new_metric
  description: "New business metric calculation"
  incremental: true
  class_name: ETLBase
  input_tables:
    source_table: bronze_source_table
  output_table_name: fct_new_metric
  partition_by:
    -  year_month
  unload: true

```

  

### Step 3: Test Your Job

  

1.  **Run the specific job**:

```bash

python main.py --silver

```

  

2.  **Check the logs** for any errors or issues

  

3.  **Verify the output** in the Iceberg catalog:

```sql

SELECT  *  FROM dev_catalog.etrdatamart.silver_fct_new_metric LIMIT  10;

```

  

### Step 4: Generate Updated Artifacts

  

After adding your job, regenerate all artifacts:

  

```bash

python  generate.py  all

```

  

This will update:

- Airflow DAGs with your new job

- Documentation with the new table

- Layer scripts with the new module

- Notebook with the new processing logic

  

### Step 5: Deploy to Airflow

  

1.  **Copy the generated DAG** to your Airflow DAGs folder

2.  **Update Airflow configuration** if needed

3.  **Test the DAG** in the Airflow UI

4.  **Schedule the job** according to your requirements

  

---

  

## Best Practices

  

### ETL Job Development

  

1.  **Follow naming conventions**:

- Bronze: `bronze_<source_system>_<table_name>`

- Silver: `silver_<fact/dim>_<business_concept>_<granularity>`

- Domain: `domain_<domain_area>_<granularity>`

  

2.  **Use incremental processing** when possible:

```yaml

incremental: true

partition_by:

-  year_month

```

  

3.  **Include proper logging**:

```python

logging.info(f"{self.logging_string} - Processing started...")

```

  

4.  **Handle data quality**:

- Add data validation

- Handle null values appropriately

- Include data quality metrics

  

### Configuration Management

  

1.  **Keep descriptions clear and detailed**

2.  **Use consistent naming for input table aliases**

3.  **Specify partitioning strategy for large tables**

4.  **Document any special processing requirements**

  

### Performance Optimization

  

1.  **Use appropriate partitioning** for large tables

2.  **Leverage incremental processing** for time-series data

3.  **Optimize Spark configurations** based on data size

4.  **Monitor job performance** and adjust resources as needed

  

---

  

## Troubleshooting

  

### Common Issues

  

1.  **Module not found**: Check that the module name in `etl_config.yaml` matches the Python file name

2.  **Table not found**: Verify that input tables exist and are accessible

3.  **Permission errors**: Ensure AWS credentials have proper S3 and Glue permissions

4.  **Memory issues**: Adjust Spark memory settings in `spark_session.py`

  

### Debugging

  

1.  **Check Spark UI** for job execution details

2.  **Review logs** for specific error messages

3.  **Test individual components** before running full pipeline

4.  **Use incremental mode** for faster iteration during development

  

---

  

This documentation provides a comprehensive guide to understanding and working with the BOLTDBT PIPELINER ETL pipeline. The system is designed to be extensible and maintainable, allowing you to add new data sources and transformations while maintaining consistency and reliability.
