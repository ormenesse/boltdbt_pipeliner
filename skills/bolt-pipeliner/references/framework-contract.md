# Bolt Pipeliner Framework Contract

This reference describes the conventions the skill must preserve while adapting
user requirements.

## Runtime Source of Truth

`configs/etl_config.yaml` declares:

- `configs.output_location` and `configs.flatfile_location`
- `configs.schema` and `configs.catalog`
- default incremental settings
- the ordered `layers` mapping
- one top-level job list for every declared layer

Each job normally contains:

```yaml
- module: silver_orders
  description: "Build the order fact table."
  class_name: ETLBaseParquetPandas
  input_tables:
    orders: bronze_orders
  output_table_name: orders
  partition_by: [order_month]
  incremental: true
  tests:
    - not_null: [order_id]
    - unique: [order_id]
```

The required job keys are `module`, `input_tables`, and
`output_table_name`. `class_name` defaults to `ETLBase` when omitted.

## Job Contract

The module named by `module` is imported from the directory declared for its
layer. It must expose:

```python
def process_data(self, input_tables):
    ...
```

`input_tables` is a dictionary keyed by the aliases declared in YAML. Its
values are already-loaded engine dataframes. A job should return the dataframe
that the selected base class will persist, unless it deliberately handles its
own write with `unload: false` and the base-class helpers.

## Layer Inputs

- `flatfile` values are relative file paths under `flatfile_location`.
- A dotted value such as `raw.crm_account` is a shared-catalog reference.
- A normal internal dependency is the upstream layer name plus its output name,
  such as `bronze_orders` or `silver_order_features`.
- Bare table references are valid where the framework can resolve them, but
  fully-qualified layer names are clearer and preferred for generated projects.

## Base Classes

Use the built-in class that matches the job's engine and storage:

| Class | Use |
| --- | --- |
| `ETLBase` | Spark with Iceberg-style tables |
| `ETLBaseDelta` | Spark with Delta |
| `ETLBaseParquet` | Spark with Parquet |
| `ETLBaseParquetPandas` | Pandas with Parquet |
| `ETLBaseParquetPolars` | Polars with Parquet |

Per-job `class_name` values may mix compatible engines in one project. A
custom class must be a valid dotted import path.

## Canonical Layout

| Purpose | Path |
| --- | --- |
| Configuration | `configs/` |
| Pipeline jobs | `etl/` |
| Shared functions | `macros/` |
| Project tests | `tests/` |
| Production ML code | `models/` |
| ML experimentation | `model_notebooks/` |
| Generated artifacts | `outputs/` |
| Vendored framework | `_boltpipeliner/` |

The conventional layer directories are `etl/_flatfile`, `etl/0_bronze`,
`etl/1_silver`, `etl/2_gold`, and `etl/3_diamond`. Custom layer directories
remain below `etl/` and must be declared in `layers:`.

## Generated Artifacts

`bolt generate` can create:

- `outputs/airflow/{dags,code}/`
- `outputs/documentation/`
- `outputs/layers/`
- `outputs/notebook/etl_jobs_notebook.ipynb`
- `outputs/schema/`

These files are derived from configuration and job modules. The skill should
regenerate them after source changes and should not use them as the place for
business logic.

The current ETL notebook generator emits Spark setup cells. Only generate that
artifact for a project whose engine and runtime support those cells. The
`model_notebooks/` scaffold is a separate ML experimentation surface and has
engine-specific imports.

## Environment Profile

`configs/bolt_environment.yaml` is a non-secret decision record for the
Bolt-Pipeliner skill. It is not a replacement for `etl_config.yaml`, and the
runtime does not need it to execute jobs. It may contain:

- engine and default base class
- declared layers and paths
- execution environment and Spark profile
- data locations
- catalog and schema
- ML and artifact preferences
- user naming or project conventions

Never put passwords, tokens, private keys, or credential values in the profile.
Store environment-variable names or secret-manager identifiers instead.
