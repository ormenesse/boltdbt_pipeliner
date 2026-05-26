# bolt_pipeliner

A config-driven ETL framework for **Apache Spark + Iceberg**, **Pandas**, and **Polars**, with sibling base classes for **Spark + Delta** and **Spark + Parquet**. Pipelines are declared in a single YAML file and executed through one CLI:

```bash
bolt init my_project --preset medallion
cd my_project
bolt run --silver
bolt test
bolt generate documentation
```

The framework is inspired by dbt's `tests:` ergonomics but stays Python-first: jobs are plain modules exposing a `process_data(self, input_tables)` function, the runtime wires them onto a shared `ETLBase`, and downstream artifacts (Airflow DAGs, HTML docs, standalone layer scripts, notebooks, Snowflake DDLs) are regenerated from the same config.

> Browsing the repo? The **package code** lives under [`src/bolt_pipeliner/`](./src/bolt_pipeliner). Sample projects (a runnable demo plus two large reference projects) live under [`examples/`](./examples).

---

## Table of contents

1. [Installation](#installation)
2. [Quick start — `bolt init`](#quick-start--bolt-init)
3. [CLI reference](#cli-reference)
4. [Config schema (`etl_config.yaml`)](#config-schema-etl_configyaml)
5. [Base classes (engine selection)](#base-classes-engine-selection)
6. [Writing an ETL job](#writing-an-etl-job)
7. [Incremental processing](#incremental-processing)
8. [Data-quality tests (`bolt test`)](#data-quality-tests-bolt-test)
9. [Code generation (`bolt generate`)](#code-generation-bolt-generate)
10. [Spark session profiles](#spark-session-profiles)
11. [Macros (reusable transforms)](#macros-reusable-transforms)
12. [Documentation flow without Spark](#documentation-flow-without-spark)
13. [Project layout](#project-layout)
14. [Troubleshooting](#troubleshooting)

---

## Installation

Requires Python ≥ 3.10.

```bash
pip install -e .

# add dev tooling for pytest / ruff / mypy
pip install -e ".[dev]"

# add Databricks Connect / PySpark when you need Spark locally
pip install -e ".[spark]"
```

The install registers the `bolt` console script.

---

## Quick start — `bolt init`

Interactive scaffolder:

```bash
bolt init my_project
```

The wizard asks:

| Question | Choices |
|---|---|
| Architecture | flat • medallion (bronze/silver/gold) • diamond (bronze/silver/gold/diamond) • custom |
| Engine | pyspark • pandas • polars |
| Spark profile (pyspark only) | local • databricks • emr • glue • gcp • azure • k8s |
| Execution env | terminal • notebook • airflow • databricks-jobs |
| ML training layer | yes / no |

Skip the prompts with a preset:

```bash
bolt init my_project --preset minimal      # pandas, flatfile + bronze
bolt init my_project --preset medallion    # pyspark/local, bronze/silver/gold
bolt init my_project --preset diamond      # full medallion + diamond + ML, airflow
bolt init my_project --preset pandas       # pandas medallion, notebook
bolt init my_project --preset polars       # polars medallion, notebook
```

The scaffolder writes:

```
my_project/
├── configs/
│   ├── etl_config.yaml
│   └── spark/<profile>.toml          # only when engine=pyspark
├── etl/
│   ├── _flatfile/flatfile_example.py
│   ├── 0_bronze/bronze_example.py
│   ├── 1_silver/silver_example.py
│   └── 2_gold/gold_example.py
├── macros/__init__.py
├── models/train_example.py            # only when ML is enabled
├── tests/test_smoke.py
└── README.md
```

It refuses to write into a non-empty directory.

---

## CLI reference

```
bolt init      PROJECT_NAME [--path PATH] [--preset NAME]
bolt run       [--config PATH] [--flatfile|--bronze|--silver|--gold|--diamond]
bolt test      [--config PATH] [--layer L] [--module M]
bolt generate  {airflow|documentation|layers|notebook|snowflakeddl|all} [--config PATH]
```

| Command | What it does |
|---|---|
| `bolt init` | Interactive project scaffolder (above). |
| `bolt run` | Walks the layers declared in `configs/etl_config.yaml` and executes every job in dependency order. Pick a subset with the layer flags. |
| `bolt test` | Runs the `tests:` block on each job. Exits non-zero on failure. |
| `bolt generate` | Regenerates Airflow DAGs, HTML docs, standalone layer scripts, the notebook, and Snowflake DDLs from the config. Use `all` to run all of them. |

`bolt --help` and `bolt <subcommand> --help` print the full option set.

---

## Config schema (`etl_config.yaml`)

```yaml
configs:
  output_bucket: "s3://my_project/tables/"
  flatfile_bucket: "s3://my_project/flatfiles/"
  schema: my_project          # destination schema (Iceberg namespace, Snowflake schema, …)
  catalog: dev_catalog        # destination catalog for non-bronze reads/writes
  incremental_column: year_month   # optional; default "year_month" for Iceberg
                                   # base, "yearMonth" for Pandas/Polars bases

layers:
  flatfile: etl/_flatfile
  bronze:   etl/0_bronze
  silver:   etl/1_silver
  gold:     etl/2_gold

flatfile:
  - module: flatfile_storm_events
    description: "NOAA storm events."
    class_name: ETLBase                  # picks the Spark+Iceberg base
    input_tables:
      storm_events: "storm_events.csv"
    output_table_name: storm_events

silver:
  - module: silver_fct_state_gas_cpi_monthly
    description: "Monthly CPI + gas prices by state."
    class_name: ETLBase
    incremental: true
    input_tables:
      cpi: flatfile_cpi_regional
      gas: flatfile_gas_prices
    output_table_name: fct_state_gas_cpi_monthly
    partition_by: [year_month]
    tests:
      - not_null: [year_month, state]
      - unique:   [year_month, state]
      - row_count: { min: 1 }
      - freshness: { column: year_month, max_age_days: 90 }
```

### Per-job keys

| Key | Required | Notes |
|---|---|---|
| `module` | yes | Filename of the job module inside the layer's directory (no `.py`). |
| `input_tables` | yes | Dict of alias → upstream table or file. Peco-style `_input_tables:` is normalized to `input_tables:` at load time. |
| `output_table_name` | yes | Becomes `{layer}_{output_table_name}` in the destination. |
| `class_name` | no (default `ETLBase`) | Picks the base class. Built-ins: `ETLBase`, `ETLBaseDelta`, `ETLBaseParquet`, `ETLBaseParquetPandas`, `ETLBaseParquetPolars`. Also accepts dotted paths like `mypkg.bases.MyCustom`. |
| `partition_by` | no | List of column names. |
| `incremental` | no (default false) | See [Incremental processing](#incremental-processing). |
| `unload` | no (default true) | If false, the runtime won't call `unload_data` — useful for manual partition writes. |
| `description` | no | Free text; surfaces in `bolt generate documentation`. |
| `tests` | no | See [Data-quality tests](#data-quality-tests-bolt-test). |

---

## Base classes (engine selection)

Five sibling base classes ship in `bolt_pipeliner.bases.*`. They expose the same lifecycle methods (`check_if_tables_exists_find_yearmonths` → `load_data` → `process_data` → `unload_data`); jobs don't subclass them — the runner picks one per job via the YAML `class_name:` key.

| `class_name` | Engine | Storage | When to use |
|---|---|---|---|
| `ETLBase` (default) | PySpark | Iceberg (Glue) | Large-scale ETL with ACID Iceberg tables. |
| `ETLBaseDelta` | PySpark | Delta (Synapse) | Synapse / Databricks Delta lake. |
| `ETLBaseParquet` | PySpark | Parquet on S3 | Spark without a metastore. |
| `ETLBaseParquetPandas` | Pandas + PyArrow | Parquet | Notebook / single-node ETL. |
| `ETLBaseParquetPolars` | Polars + PyArrow | Parquet | Single-node ETL with Polars ergonomics. |

Engines are imported **lazily**: importing `bolt_pipeliner` does not pull in PySpark / Polars / Pandas. Engine modules are loaded only when a job actually instantiates one. You can therefore run a pure-Pandas project without installing PySpark.

To register your own base class, point `class_name:` at a dotted path:

```yaml
- module: silver_custom
  class_name: mypkg.bases.MyAuditingBase
  input_tables: { src: bronze_src }
  output_table_name: custom
```

---

## Writing an ETL job

Every job module exports one top-level function:

```python
# etl/1_silver/silver_fct_account_calls_monthly.py
from pyspark.sql import functions as F

def process_data(self, input_tables):
    """`self` is the ETLBase instance, so you can call self.spark, self.year_months,
    self._create_table, self._replace_table_partitions, etc.
    """
    calls = input_tables["t_agent_calls"]
    return (
        calls
        .groupBy("account_id", "year_month")
        .agg(F.count("*").alias("call_count"))
    )
```

The runner monkey-patches your function onto the `ETLBase` instance via `types.MethodType`, so `self` exposes:

| Attribute / method | Purpose |
|---|---|
| `self.spark` | The Spark session (Spark bases only). |
| `self.input_tables` | Dict of alias → DataFrame, already loaded. |
| `self.year_months` | The incremental window (list of YYYYMM ints), or `None` for a full run. |
| `self.partition_by`, `self.incremental`, `self.unload` | Echo of the YAML config. |
| `self._create_table(df)` / `self._replace_table_partitions(df)` | Manual write helpers. |
| `self.iceberg_table` / `self._write_table` / `self.parquet_path` / `self.dataset_path` | Destination identifier (varies by base). |
| `self.logging_string` | A short `"<layer> <output_table_name>"` label for logs. |

A Pandas job looks the same but returns a `pd.DataFrame`; a Polars job returns a `pl.DataFrame`. The base class decides how to persist it.

---

## Incremental processing

Set `incremental: true` and list a partition column whose name matches `incremental_column` (default `year_month`):

```yaml
- module: silver_fct_calls
  incremental: true
  partition_by: [year_month]
```

When the output table already exists, `ETLBase.run()` computes `self.year_months` as `[current_month - 3 … current_month]`. The base then filters `processed_df` by `year_month ∈ self.year_months` before `overwritePartitions()`.

**Requirements:** the returned DataFrame must include the configured incremental column, and that column must be in `partition_by`. If a job does not fit this monthly model, either set `incremental: false` or set `unload: false` and write partitions yourself inside `process_data`, then return an empty DataFrame.

### Advanced — manual partition unloading

For memory-heavy jobs, process month-by-month and write each partition yourself:

```python
def process_data(self, input_tables):
    raw = input_tables["raw"]
    months_to_process = self.year_months or _list_all_months(raw)
    for ym in months_to_process:
        chunk = transform_one_month(raw, ym)
        if not self.table_exists:
            self._create_table(chunk)
            self.table_exists = True
        else:
            self._replace_table_partitions(chunk)
    return self.spark.createDataFrame([], chunk.schema)   # empty → unload_data no-ops
```

Pair it with `unload: false` in YAML.

---

## Data-quality tests (`bolt test`)

Declare checks under each job's `tests:` block (dbt-style):

```yaml
silver:
  - module: silver_fct_account_calls_monthly
    output_table_name: fct_account_calls_monthly
    tests:
      - not_null:  [year_month, account_id]
      - unique:    [year_month, account_id]
      - row_count: { min: 1 }
      - freshness: { column: year_month, max_age_days: 90 }
      - schema:    [year_month, account_id, call_count]
```

Built-in checks (all five work uniformly on Spark / Pandas / Polars):

| Check | Parameters |
|---|---|
| `not_null` | `columns: [str]` |
| `unique` | `columns: [str]` (composite key) |
| `row_count` | `min: int = 1`, `max: int \| None` |
| `schema` | `expected: [str]` (extras allowed) |
| `freshness` | `column: str`, `max_age_days: int` (accepts YYYYMM int or date) |

Run them:

```bash
bolt test                     # every job in every layer
bolt test --layer silver
bolt test --module fct_account_calls_monthly
```

`bolt test` exits non-zero if any check fails, so it slots straight into CI.

### Notebook usage

Each `TestResult` implements `_repr_html_()`, so the results render with colored PASS/FAIL banners in Jupyter:

```python
from bolt_pipeliner.testing import run_checks
results = run_checks(df, [{"not_null": ["year_month"]}, {"row_count": {"min": 1}}])
results          # rendered inline as HTML
```

---

## Code generation (`bolt generate`)

`bolt generate <target> [--config PATH]` regenerates downstream artifacts from `etl_config.yaml`. Targets:

| Target | Output | What you get |
|---|---|---|
| `airflow` | `outputs/airflow/{code,dags}/` | One DAG per layer + one standalone Spark script per job. The DAG template uses plain Airflow operators; swap in `EmrContainerOperator` / `DatabricksSubmitRunOperator` / `KubernetesPodOperator` as needed. |
| `documentation` | `outputs/documentation/` | HTML index + per-table pages with Mermaid lineage. Always emits `outputs/schema/schema.py` for the [Spark-free documentation flow](#documentation-flow-without-spark). |
| `layers` | `outputs/layers/<layer>.py` | One executable script per layer, inlining every job in dependency order. Useful for ad-hoc runs without Airflow. |
| `notebook` | `outputs/notebook/etl_jobs_notebook.ipynb` | A Jupyter notebook with one cell per job (plus Spark session + ETLBase setup cells). |
| `snowflakeddl` | `outputs/snowflake_ddls/` | `CREATE TABLE` DDLs derived from `outputs/schema/schema.csv`. |
| `all` | (all of the above) | |

The generators read templates from the package (`src/bolt_pipeliner/templates/`); they're engine-agnostic and don't ship any cloud-specific code paths.

### Smart dependency resolution

The order of jobs inside each layer in YAML is **irrelevant** — the generators build a DAG by matching each job's `input_tables` values against other jobs' `output_table_name` (prefixed with the layer name) and topologically sort.

---

## Spark session profiles

`bolt_pipeliner.sessions.create_session(profile)` dispatches to one module per runtime under `bolt_pipeliner.sessions/`:

| Profile | Module | Status |
|---|---|---|
| `local` | `sessions/local.py` | Implemented — returns the active SparkSession or builds one. |
| `databricks`, `emr`, `glue`, `gcp`, `azure`, `k8s` | `sessions/<profile>.py` | Stubs today; planned. |

Override via `BOLT_SPARK_PROFILE` (read by the generated standalone scripts) or by editing `configs/spark/<profile>.toml`.

```toml
# configs/spark/local.toml
[runtime]
target = "local"

[spark]
"spark.sql.shuffle.partitions" = 200
"spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
```

---

## Macros (reusable transforms)

Project-local reusable transforms live in `macros/` and are plain Python — no DSL:

```python
# macros/dates.py
def month_floor(df, column):
    """Round `column` down to the first of the month. Engine-aware via isinstance."""
    ...
```

```python
# etl/1_silver/silver_invoice.py
from macros.dates import month_floor

def process_data(self, input_tables):
    return month_floor(input_tables["raw"], "issue_date")
```

The framework deliberately does not ship a macro DSL or registry. Use plain imports.

---

## Documentation flow without Spark

`bolt generate documentation` always emits a Spark-free schema-extraction script alongside the HTML so notebook-only developers can produce schemas without a local Spark install:

1. `bolt generate documentation`
   → writes `outputs/schema/schema.py` and the HTML (falling back to whatever `schema.csv` exists, or empty if none).
2. Copy `schema.py` into the environment that has Spark (Databricks notebook, EMR shell, …) and run it. It prints a CSV.
3. Save that CSV to `outputs/schema/schema.csv`.
4. Re-run `bolt generate documentation` — the HTML now picks up the real column definitions, and `bolt generate snowflakeddl` can derive Snowflake DDLs from the same CSV.

---

## Project layout

```
my_project/
├── configs/
│   ├── etl_config.yaml           # source of truth — layers, jobs, tests
│   ├── style_config.yaml         # colors used by `bolt generate documentation`
│   └── spark/<profile>.toml      # cluster overrides per runtime profile
├── etl/
│   ├── _flatfile/                # raw ingestion
│   ├── 0_bronze/                 # cleaned raw
│   ├── 1_silver/                 # business logic
│   └── 2_gold/                   # domain-specific facts / marts
├── macros/                       # reusable Python transforms
├── models/                       # optional ML training jobs
├── tests/                        # pytest unit tests (project-specific)
└── outputs/                      # generated; gitignored
    ├── airflow/{dags,code}/
    ├── documentation/
    ├── layers/
    ├── notebook/
    ├── schema/
    └── snowflake_ddls/
```

Framework code itself lives under `src/bolt_pipeliner/`:

```
src/bolt_pipeliner/
├── runner.py                     # job loop
├── bases/                        # five sibling ETLBase variants
├── sessions/                     # Spark profile dispatch
├── config/loader.py              # YAML loader + key normalization
├── generators/                   # airflow / documentation / layers / notebook / snowflake_ddl
├── templates/{airflow,docs}/     # bundled templates
├── testing/                      # data-quality checks + runner
└── cli/                          # typer app (init / run / generate / test)
```

And sample projects live under `examples/`:

```
examples/
├── demo/        # runnable PySpark + Iceberg medallion (used in this repo's CI)
├── entergy/    # large production-scale snapshot — read-only reference
└── peco/       # multi-engine reference (PySpark + Pandas + Polars on Parquet)
```

See [`examples/README.md`](./examples/README.md) for a guided tour.

---

## Troubleshooting

**`ImportError: cannot import name 'AnalyzeArgument' from 'pyspark.sql.udtf'`**
Your local PySpark install is older than the package's expected version. Reinstall PySpark or pin to a version matching your cluster.

**`bolt run` finds no jobs for a layer**
Check that the layer is declared under `layers:` in `etl_config.yaml` *and* has a matching top-level section (e.g. `silver:`). The loader treats placeholder values like `silver: ...` as empty layers.

**Generated documentation has empty schema columns**
Spark wasn't reachable. Run `outputs/schema/schema.py` in your Spark environment, save the printed CSV to `outputs/schema/schema.csv`, and rerun `bolt generate documentation`.

**Adding a new layer**
1. Add `mylayer: etl/9_mylayer` under `layers:` in `etl_config.yaml`.
2. Create `etl/9_mylayer/` with one or more job modules exposing `process_data(self, input_tables)`.
3. List the jobs under the new top-level `mylayer:` section.
4. Run `bolt run --mylayer` — wait, layer flags are baked into the CLI today (`--flatfile`, `--bronze`, `--silver`, `--gold`, `--diamond`). For arbitrary layer names, run `bolt run` (all layers) or invoke the runner programmatically: `from bolt_pipeliner.runner import run; run("configs/etl_config.yaml", layers=["mylayer"])`.

---

## Contributing

```bash
pip install -e ".[dev]"
ruff check src/ tests/
mypy src/bolt_pipeliner
pytest -q
```

Pull requests should:
- Keep changes minimal and scoped.
- Add tests for any new behavior (the suite covers config loading, runner resolution, generators, the CLI, and all data-quality checks across Pandas + Polars).
- Avoid breaking the lazy-import invariant — importing `bolt_pipeliner` must not pull in PySpark / Polars / Pandas at module load time. The `tests/test_package_imports_lazily.py` suite enforces this.
