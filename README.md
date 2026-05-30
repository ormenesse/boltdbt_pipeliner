# Bolt Pipeliner

[Web Documentation can be found here](https://boltpipeliner-documentation.vercel.app/docs)

A config-driven ETL framework for **Apache Spark + Iceberg**, **Pandas**, and **Polars**, with sibling base classes for **Spark + Delta** and **Spark + Parquet**. Pipelines are declared in a single YAML file and executed through one CLI:

```bash
bolt init my_project --preset medallion
cd my_project
bolt run --silver
bolt test
bolt generate documentation
```

The framework is inspired by dbt's `tests:` ergonomics but stays Python-first: jobs are plain modules exposing a `process_data(self, input_tables)` function, the runtime wires them onto a shared `ETLBase`, and downstream artifacts (Airflow DAGs, HTML docs, standalone layer scripts, notebooks) are regenerated from the same config.

> Browsing the repo? The **package code** lives under [`src/bolt_pipeliner/`](./src/bolt_pipeliner). Sample projects (Spark/Pandas/Polars runnable demos plus larger reference projects) live under [`examples/`](./examples).

---

## Table of contents

1. [Installation](#installation)
2. [Quick start — `bolt init`](#quick-start--bolt-init)
3. [Self-contained projects (vendored copy + shims)](#self-contained-projects-vendored-copy--shims)
4. [CLI reference](#cli-reference)
   - [Selecting jobs to run](#selecting-jobs-to-run)
5. [Config schema (`etl_config.yaml`)](#config-schema-etl_configyaml)
6. [Base classes (engine selection)](#base-classes-engine-selection)
7. [Writing an ETL job](#writing-an-etl-job)
8. [Incremental processing](#incremental-processing)
9. [Data-quality tests (`bolt test`)](#data-quality-tests-bolt-test)
10. [Code generation (`bolt generate`)](#code-generation-bolt-generate)
11. [Spark session profiles](#spark-session-profiles)
12. [Macros (reusable transforms)](#macros-reusable-transforms)
13. [ML training (`models/` + `model_notebooks/`)](#ml-training-models--model_notebooks)
14. [Documentation flow without Spark](#documentation-flow-without-spark)
15. [Project layout](#project-layout)
16. [Troubleshooting](#troubleshooting)

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
| Output location | URI/path for generated layer outputs (local default `data/layers`; cloud defaults ship prefilled prefixes like `s3://<bucket>/<path>/...`) |
| Flatfile location | URI/path for inbound flatfiles (local default `data/flatfiles`; cloud defaults ship prefilled prefixes like `gs://<bucket>/<path>/...`) |
| Execution env | terminal • notebook • airflow • databricks-jobs |
| ML training layer | yes / no (when yes, also offers to add a `diamond` layer if missing) |

Skip the prompts with a preset:

```bash
bolt init my_project --preset minimal      # pandas, flatfile + bronze
bolt init my_project --preset medallion    # pyspark/local, bronze/silver/gold
bolt init my_project --preset diamond      # full medallion + diamond + ML, airflow
bolt init my_project --preset pandas       # pandas medallion, notebook
bolt init my_project --preset polars       # polars medallion, notebook

bolt init my_project --preset medallion --no-vendor   # skip the vendored copy
```

The scaffolder writes:

```
my_project/
├── configs/
│   ├── etl_config.yaml
│   ├── style_config.yaml                   # colors for `bolt generate documentation`
│   └── spark/<profile>.toml                # only when engine=pyspark
├── etl/
│   ├── _flatfile/flatfile_example.py
│   ├── 0_bronze/bronze_example.py
│   ├── 1_silver/silver_example.py
│   ├── 2_gold/gold_example.py
│   └── 3_diamond/diamond_example.py        # only on diamond architecture / ML
├── macros/__init__.py
├── models/train_example.py                 # only when ML is enabled
├── model_notebooks/                        # only when ML is enabled
│   ├── README.md
│   └── train_example.ipynb
├── tests/test_smoke.py
├── _boltpipeliner/                         # vendored copy of bolt_pipeliner
│   ├── README.md
│   └── bolt_pipeliner/...
├── bolt.py                                 # `python bolt.py <subcommand>`
├── main.py                                 # `python main.py [--bronze ...]`
├── generate.py                             # `python generate.py <target>`
└── README.md
```

It refuses to write into a non-empty directory.

`configs/style_config.yaml` is **always** scaffolded (it's required by `bolt generate documentation`) and is pre-populated with a color palette matching whichever layers you picked.

---

## Self-contained projects (vendored copy + shims)

`bolt init` vendors a full copy of `bolt_pipeliner` into `<project>/_boltpipeliner/` and emits three shim scripts at the project root:

| Shim | Equivalent to | Use it when |
|---|---|---|
| `python bolt.py …` | `bolt …` | You want the full CLI surface (`run`, `generate`, `test`, `init`). |
| `python main.py …` | `bolt run …` | Quick layer runs: `python main.py --bronze --silver`. |
| `python generate.py …` | `bolt generate …` | Regenerate artifacts: `python generate.py documentation`. |

Each shim prepends `_boltpipeliner/` to `sys.path` before importing `bolt_pipeliner`, so **the vendored copy wins over any pip-installed version**. That means:

- The project runs end-to-end on a fresh clone — no `pip install bolt_pipeliner` required.
- The version of the framework that ships with the repo is the version that runs, so a checkout from six months ago still produces the same artifacts.
- Downstream consumers (CI, Airflow workers, Docker images) only need `pip install -r requirements.txt` for engine deps (PySpark / Pandas / Polars) plus YAML/Typer, not the framework itself.

If you'd rather rely on a pip-installed copy, pass `--no-vendor`:

```bash
bolt init my_project --preset medallion --no-vendor
```

The shims are still emitted; they fall back to the installed package when `_boltpipeliner/` is absent.

**Refreshing the vendored copy.** Re-run `bolt init` in a fresh directory, or copy the new `src/bolt_pipeliner/` tree over `_boltpipeliner/bolt_pipeliner/` after upgrading. Don't hand-edit files under `_boltpipeliner/bolt_pipeliner/` — patch the upstream package instead.

---

## CLI reference

```
bolt init      PROJECT_NAME [--path PATH] [--preset NAME] [--vendor/--no-vendor]
bolt run       [--config PATH] [--flatfile|--bronze|--silver|--gold|--diamond]
                              [--select SEL] [--layer L] [--verbose]
bolt test      [--config PATH] [--layer L] [--module M]
bolt generate  {airflow|documentation|layers|notebook|all} [--config PATH]
```

| Command | What it does |
|---|---|
| `bolt init` | Interactive project scaffolder (above). `--no-vendor` skips the vendored copy. |
| `bolt run` | Walks the layers declared in `configs/etl_config.yaml` and executes every job in dependency order. Pick a subset with `--bronze` / `--silver` / …, or with `--select` / `-s` for dbt-style table-level selection (see [Selecting jobs to run](#selecting-jobs-to-run)). |
| `bolt test` | Runs the `tests:` block on each job. Exits non-zero on failure. |
| `bolt generate` | Regenerates Airflow DAGs, HTML docs, standalone layer scripts, and the notebook from the config. Use `all` to run all of them. |

`bolt --help` and `bolt <subcommand> --help` print the full option set. Inside a scaffolded project you can substitute `python bolt.py …` / `python main.py …` / `python generate.py …` (see [Self-contained projects](#self-contained-projects-vendored-copy--shims)).

Use `bolt run --verbose` to print each resolved job/module as it executes.

### Selecting jobs to run

`bolt run --select <selector>` (or `-s`) targets specific tables — and, dbt-style, their upstream/downstream neighbourhood:

| Selector | Meaning |
|---|---|
| `silver_orders`  | just that one job |
| `+silver_orders` | upstream-of-`silver_orders` **+** `silver_orders` |
| `silver_orders+` | `silver_orders` **+** downstream-of-`silver_orders` |
| `+silver_orders+` | upstream **+** `silver_orders` **+** downstream |

A selector accepts either the full `{layer}_{output_table_name}` form (`silver_orders`) or a bare `output_table_name` (`orders`) when only one layer exposes it. When the same `output_table_name` lives in multiple layers, pass `--layer / -l` to disambiguate:

```bash
bolt run -s +silver_x          # rebuild silver_x and everything it depends on
bolt run -s bronze_a+          # rebuild bronze_a and every silver/gold that consumes it
bolt run -s +silver_x+         # full rebuild around silver_x
bolt run -s orders -l silver   # bare name + layer constraint
```

`--select` is mutually exclusive with `--bronze` / `--silver` / `--gold` / `--diamond` / `--flatfile`. Use `--layer` *alongside* `--select` to disambiguate bare names, or **standalone** as a synonym for `--<layer>`:

```bash
bolt run -l silver             # equivalent to `bolt run --silver`
```

Selection respects YAML layer order: flatfile → bronze → silver → gold → diamond. Within each layer, jobs run in the order they appear in `etl_config.yaml`. External references (flatfile paths, shared-catalog reads like `raw.crm_account`) are skipped from the dependency graph — they're not jobs this project schedules.

---

## Config schema (`etl_config.yaml`)

```yaml
configs:
  output_location: "data/layers"
  flatfile_location: "data/flatfiles"
  schema: my_project          # destination schema (Iceberg namespace, Snowflake schema, …)
  catalog: dev_catalog        # destination catalog for non-bronze reads/writes
  incremental_column: anomes
  incremental_type: int           # int | date
  incremental_unit: 3             # -1/overwrite, 0/append, or N>0 window
  incremental_date_grain: monthly # yearly | monthly | daily (for date type)

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

`output_bucket` / `flatfile_bucket` are still accepted as legacy aliases; they map to
`output_location` / `flatfile_location` automatically at load time.

### Storage locations and flatfile formats

- `output_location` and `flatfile_location` accept local paths or cloud URIs (`s3://`, `gs://`, `abfss://`, `dbfs:/`, etc.).
- `bolt init` defaults local projects to `data/layers` + `data/flatfiles`; cloud Spark profiles default to scheme-prefixed placeholders so you only fill bucket/container/path.
- Flatfile `input_tables` entries are resolved relative to `flatfile_location` when you pass a relative path.
- Pandas and Polars parquet bases support `.csv`, `.parquet`, Excel (`.xlsx`/`.xls`/etc.), and JSON (`.json`, `.jsonl`, `.ndjson`).
- JSON flatfiles in the Pandas/Polars parquet bases are loaded with `pd.json_normalize` semantics to flatten nested payloads.

### Per-job keys

| Key | Required | Notes |
|---|---|---|
| `module` | yes | Filename of the job module inside the layer's directory (no `.py`). |
| `input_tables` | yes | Dict of alias → upstream table or file. Peco-style `_input_tables:` is normalized to `input_tables:` at load time. |
| `output_table_name` | yes | Becomes `{layer}_{output_table_name}` in the destination. |
| `class_name` | no (default `ETLBase`) | Picks the base class. Built-ins: `ETLBase`, `ETLBaseDelta`, `ETLBaseParquet`, `ETLBaseParquetPandas`, `ETLBaseParquetPolars`. Also accepts dotted paths like `mypkg.bases.MyCustom`. |
| `partition_by` | no | List of column names. |
| `incremental` | no (default false) | See [Incremental processing](#incremental-processing). |
| `incremental_column` | no | Per-job override for the root `configs.incremental_column`. |
| `incremental_type` | no | Per-job override for `configs.incremental_type` (`int` or `date`). |
| `incremental_unit` | no | Per-job override for `configs.incremental_unit` (`-1`/`overwrite`, `0`/`append`, or positive window size). |
| `incremental_date_grain` | no | Per-job override for `configs.incremental_date_grain` (`yearly`, `monthly`, `daily`) when `incremental_type: date`. |
| `unload` | no (default true) | If false, the runtime won't call `unload_data` — useful for manual partition writes. |
| `description` | no | Free text; surfaces in `bolt generate documentation`. |
| `tests` | no | See [Data-quality tests](#data-quality-tests-bolt-test). |

---

## Base classes (engine selection)

Five sibling base classes ship in `bolt_pipeliner.bases.*`. They expose the same lifecycle shape (`check_if_tables_exists_find_yearmonths` [legacy hook] → `load_data` → `process_data` → `unload_data`); jobs don't subclass them — the runner picks one per job via the YAML `class_name:` key.

| `class_name` | Engine | Storage | When to use |
|---|---|---|---|
| `ETLBase` (default) | PySpark | Iceberg (Glue) | Large-scale ETL with ACID Iceberg tables. |
| `ETLBaseDelta` | PySpark | Delta (Synapse) | Synapse / Databricks Delta lake. |
| `ETLBaseParquet` | PySpark | Parquet on configurable URI/path | Spark without a metastore. |
| `ETLBaseParquetPandas` | Pandas + PyArrow | Parquet on configurable URI/path | Notebook / single-node ETL. |
| `ETLBaseParquetPolars` | Polars + PyArrow | Parquet on configurable URI/path | Single-node ETL with Polars ergonomics. |

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
    """`self` is the ETLBase instance, so you can call self.spark,
    self.incremental_policy, self.incremental_column, self._create_table,
    self._replace_table_partitions, etc.
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
| `self.incremental_policy`, `self.incremental_column` | Effective incremental mode + column resolved from root/job config. |
| `self.partition_by`, `self.incremental`, `self.unload` | Echo of the YAML config. |
| `self._create_table(df)` / `self._replace_table_partitions(df)` | Manual write helpers. |
| `self.iceberg_table` / `self._write_table` / `self.parquet_path` / `self.dataset_path` | Destination identifier (varies by base). |
| `self.logging_string` | A short `"<layer> <output_table_name>"` label for logs. |

A Pandas job looks the same but returns a `pd.DataFrame`; a Polars job returns a `pl.DataFrame`. The base class decides how to persist it.

---

## Incremental processing

Set `incremental: true` and configure the root policy once under `configs:`. You can override any of these keys per job.

```yaml
- module: silver_fct_calls
  incremental: true
  incremental_column: anomes
  incremental_type: int
  incremental_unit: 2
  partition_by: [anomes]
```

Incremental modes:

- `incremental_unit: -1` or `overwrite` → full overwrite.
- `incremental_unit: 0` or `append` → only new incremental-column values not already present are appended.
- `incremental_unit: N` (`N > 0`) → last `N` existing incremental values are refreshed, plus any newer values.

Type constraints:

- `incremental_type: int` → incremental column must be integer-like.
- `incremental_type: date` → incremental column must be date-like and constrained by `incremental_date_grain` (`yearly`, `monthly`, `daily`).

**Requirements:** the returned DataFrame must include the configured incremental column whenever `incremental: true`.

### Advanced — manual partition unloading

For memory-heavy jobs, process month-by-month and write each partition yourself:

```python
def process_data(self, input_tables):
    raw = input_tables["raw"]
    inc_col = self.incremental_column
    mode = self.incremental_policy.mode
    values_to_process = _resolve_values_from_policy(raw, inc_col, mode)
    for value in values_to_process:
        chunk = transform_one_value(raw, value, inc_col)
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
| `airflow` | `outputs/airflow/{code,dags}/` | One DAG per layer + one standalone Spark script per job. The generator auto-selects an Airflow operator family from your Spark profile (`emr/glue`, `gcp`, `azure`, `k8s`, `databricks`, or local fallback), then emits placeholders for your real cluster/resource identifiers. |
| `documentation` | `outputs/documentation/` | HTML index + per-table pages with Mermaid lineage. Always emits `outputs/schema/schema.py` for the [Spark-free documentation flow](#documentation-flow-without-spark). |
| `layers` | `outputs/layers/<layer>.py` | One executable script per layer, inlining every job in dependency order. Useful for ad-hoc runs without Airflow. |
| `notebook` | `outputs/notebook/etl_jobs_notebook.ipynb` | A Jupyter notebook with one cell per job (plus Spark session + ETLBase setup cells). |
| `all` | (all of the above) | |

The generators read templates from the package (`src/bolt_pipeliner/templates/`). Airflow DAGs include cloud-specific operator skeletons, but you still provide environment values (for example application IDs, cluster names, image URIs, connection IDs).

Every generated documentation page includes the footnote: `Created by Bolt-Pipeliner`.

### Smart dependency resolution

The order of jobs inside each layer in YAML is **irrelevant** — the generators build a DAG by matching each job's `input_tables` values against other jobs' `output_table_name` (prefixed with the layer name) and topologically sort.

---

## Spark session profiles

`bolt_pipeliner.sessions.create_session(profile)` dispatches to one module per runtime under `bolt_pipeliner.sessions/`:

| Profile | Module | Status |
|---|---|---|
| `local` | `sessions/local.py` | Implemented — returns the active SparkSession or builds one. |
| `databricks`, `emr`, `glue`, `gcp`, `azure`, `k8s` | `sessions/<profile>.py` | Stubs today; planned. |

`bolt run`, `bolt test`, notebook generation, and Airflow generation auto-load `configs/spark/<profile>.toml`. Override profile selection via `BOLT_SPARK_PROFILE` (or `configs.spark_profile` in `etl_config.yaml`).

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

## ML training (`models/` + `model_notebooks/`)

When you answer **yes** to the "Include an ML training layer?" prompt (or use `--preset diamond`), `bolt init` scaffolds two ML-related directories side-by-side:

| Directory | Role |
|---|---|
| `models/` | Production training/loading code. `train_example.py` exposes `train(features)` and `load_latest(stage)` stubs with MLflow patterns baked in (commented out by default). |
| `model_notebooks/` | Experimentation surface. `train_example.ipynb` is a minimal engine-aware notebook covering: pull features from gold → train → log to MLflow → register → load latest. |

**Diamond-layer convention.** ML jobs are conventionally placed in the `diamond` layer (downstream of `gold`). It's the natural home for training jobs that consume curated features and emit prediction tables or registered model versions. If you enable ML but pick a non-diamond architecture, the wizard offers to add a `diamond` layer automatically.

### MLflow (recommended, not required)

The generated `train_example.py` and `train_example.ipynb` reference [MLflow](https://mlflow.org) for experiment tracking and a model registry, but **the imports are commented out** — MLflow is a *suggestion*, not a hard dependency. Wire it in when you need it:

```bash
pip install mlflow
export MLFLOW_TRACKING_URI=sqlite:///mlflow.db        # local quickstart
# or point at your team's tracking server:
# export MLFLOW_TRACKING_URI=https://mlflow.your-org.example.com
mlflow ui                                              # http://localhost:5000
```

Inside a notebook or job:

```python
import mlflow, mlflow.sklearn

with mlflow.start_run():
    mlflow.log_param("engine", "pandas")
    mlflow.log_metric("auc", 0.87)
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name="example_model",
    )
```

Then load the latest registered version from a diamond-layer ETL job:

```python
model = mlflow.pyfunc.load_model("models:/example_model/Production")
```

**Without MLflow.** If you don't want the dep, pickle models to S3/GCS/disk under a versioned path and load by hash. The `load_latest()` stub in `models/train_example.py` is the right place to wire that in.

See `model_notebooks/README.md` in any scaffolded project for the full quickstart.

---

## Documentation flow without Spark

`bolt generate documentation` (or `python generate.py documentation` inside a scaffolded project) always emits a Spark-free schema-extraction script alongside the HTML so notebook-only developers can produce schemas without a local Spark install:

1. `python generate.py documentation`
   → writes `outputs/schema/schema.py` and the HTML (falling back to whatever `schema.csv` exists, or empty if none).
2. Copy `schema.py` into the environment that has Spark (Databricks notebook, EMR shell, …) and run it. It prints a CSV.
3. Save that CSV to `outputs/schema/schema.csv`.
4. Re-run `python generate.py documentation` — the HTML now picks up the real column definitions.

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
│   ├── 2_gold/                   # domain-specific facts / marts
│   └── 3_diamond/                # optional — conventional home for ML jobs
├── macros/                       # reusable Python transforms
├── models/                       # optional ML training jobs (MLflow-friendly)
├── model_notebooks/              # optional ML experimentation surface
├── tests/                        # pytest unit tests (project-specific)
├── _boltpipeliner/               # vendored copy of bolt_pipeliner (omit with --no-vendor)
├── bolt.py                       # `python bolt.py <subcommand>` — full CLI shim
├── main.py                       # `python main.py [--bronze ...]` — `bolt run` shim
├── generate.py                   # `python generate.py <target>` — `bolt generate` shim
└── outputs/                      # generated; gitignored
    ├── airflow/{dags,code}/
    ├── documentation/
    ├── layers/
    ├── notebook/
    └── schema/
```

Framework code itself lives under `src/bolt_pipeliner/`:

```
src/bolt_pipeliner/
├── runner.py                     # job loop
├── bases/                        # five sibling ETLBase variants
├── sessions/                     # Spark profile dispatch
├── config/loader.py              # YAML loader + key normalization
├── generators/                   # airflow / documentation / layers / notebook
├── templates/{airflow,docs}/     # bundled templates
├── testing/                      # data-quality checks + runner
└── cli/                          # typer app (init / run / generate / test)
```

And sample projects live under `examples/`:

```
examples/
├── demo/         # runnable PySpark + Iceberg medallion (used in this repo's CI)
├── demo_spark/   # local Spark + Parquet NYC collision ML scenario
├── demo_pandas/  # local Pandas + Parquet translation of the same scenario
└── demo_polars/  # local Polars + Parquet translation of the same scenario
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
