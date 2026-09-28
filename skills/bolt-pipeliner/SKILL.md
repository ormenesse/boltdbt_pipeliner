---
name: bolt-pipeliner
description: Create and adapt Bolt Pipeliner ETL projects using Spark, Pandas, or Polars. Use whenever a user asks to create an ETL pipeline, data lake pipeline, medallion architecture, Airflow pipeline, data-quality workflow, or Bolt Pipeliner project.
license: Apache-2.0
compatibility: Requires Python 3.10 or newer for Bolt Pipeliner projects. Ask before installing dependencies or accessing cloud resources.
---

# Bolt Pipeliner

Use this skill to turn a user's pipeline goal into a working, config-driven Bolt
Pipeliner project. The framework is Python-first: jobs are modules exposing
`process_data(self, input_tables)`, while `configs/etl_config.yaml` declares
the layers, dependencies, storage, incremental policy, and data-quality tests.

The user's business requirements and naming preferences are important. The
framework's project layout and runtime contracts are not optional. Resolve a
conflict by explaining it and asking the user before changing the design.

## Operating Rules

- Inspect the repository before editing anything.
- Determine whether the request targets a new project or an existing project.
- Ask questions conversationally. Use the host agent's question or approval
  tool when available, but do not assume that a questionary terminal session is
  interactive.
- Ask only questions that are not already answered by project files or the
  persisted environment profile.
- Present the resolved environment and pipeline plan before making substantial
  changes.
- Never read or print secret values from `.env`, shell configuration, or cloud
  credential files. Record only secret names or references to a secret manager.
- Do not overwrite existing files, generated artifacts, or user changes without
  explicit approval.
- Do not edit files under `_boltpipeliner/`; patch the upstream project instead.

Read [the framework contract](references/framework-contract.md) before creating
or moving files. Read [the questionnaire](references/questionnaire.md) when
collecting setup answers.

## Workflow

### 1. Inspect the project

For an existing project, inspect at least:

- `git status --short`
- `configs/bolt_environment.yaml`, if present
- `configs/etl_config.yaml`
- `configs/spark/*.toml`, if present
- `pyproject.toml`, `requirements*.txt`, or the active environment metadata
- `etl/`, `macros/`, `models/`, `model_notebooks/`, `tests/`, and `outputs/`

For a new project, confirm the target path and whether it is empty. Do not run
`bolt init` against a non-empty directory because the scaffolder intentionally
refuses to do so.

### 2. Interview the user

Collect the decisions described in `references/questionnaire.md`. At minimum,
resolve the project goal, source data, engine, storage format, layers, runtime,
locations, incremental policy, quality checks, orchestration, and optional ML
requirements.

Ask for sample files, schemas, or precise transformation rules when the user
wants implementation rather than a skeleton. Generate real transformations
only when they are grounded in those inputs. Otherwise create a clear TODO
scaffold and identify the missing information.

### 3. Confirm the environment

Show a concise summary containing:

- project root and whether it is new or existing
- engine and per-job base class choices
- layer order and filesystem paths
- input and output locations
- catalog, schema, and incremental policy
- execution environment, Spark profile, and orchestrator
- requested tests and generated artifacts
- dependency and vendoring choices

Wait for confirmation before modifying an existing project or generating
business logic.

### 4. Scaffold or adapt

For a new project:

- Choose the closest `bolt init` preset: `minimal`, `medallion`, `diamond`,
  `pandas`, or `polars`.
- Use `--vendor` or `--no-vendor` according to the user's preference.
- Run the command only after confirming the target is empty.
- For custom layers or storage formats, start from the closest preset and then
  update the framework files deliberately.

For an existing project:

- Treat the existing `etl_config.yaml` and directory tree as evidence, not as
  disposable scaffolding.
- Preserve compatible conventions and make the smallest change that satisfies
  the request.
- If `bolt_environment.yaml` is absent, derive what can be derived, ask for the
  missing decisions, and create the profile after confirmation.

After scaffolding or before adapting an existing project, re-read the files
from disk. Do not rely only on the conversation or on assumptions about what a
CLI preset wrote.

### 5. Implement the pipeline

- Put every scheduled pipeline job under the declared `etl/<layer>/` path.
- Give each job a matching `module` entry in `configs/etl_config.yaml`.
- Implement the `process_data(self, input_tables)` contract and use the YAML
  aliases when reading inputs.
- Put reusable, project-local transformations in `macros/` and import them
  from jobs.
- Put project-specific data-quality checks in the job's `tests:` block and
  broader unit tests under `tests/`.
- Put production ML code in `models/` and ML experimentation in
  `model_notebooks/`.
- Keep generated documentation, DAGs, layer scripts, schema files, and ETL
  notebooks under `outputs/`; regenerate them instead of hand-editing them.
- Use the engine-appropriate base class. Do not add Spark imports to Pandas or
  Polars jobs unless the user explicitly needs a mixed-engine project.

When source data or schemas are available, implement the transformations,
partitions, incremental behavior, and checks. Run a small local validation when
possible before claiming the pipeline is complete.

### 6. Generate and validate

From the project root, use the vendored shim when present, otherwise the
installed `bolt` command:

```bash
python bolt.py test --config configs/etl_config.yaml
python bolt.py generate documentation --config configs/etl_config.yaml
python bolt.py generate all --config configs/etl_config.yaml
```

Use `main.py` or `bolt run` for execution only after the configuration and
imports validate. Run the bundled layout validator when available:

```bash
python skills/bolt-pipeliner/scripts/validate_layout.py .
```

If the skill was installed into an agent directory, use the validator from the
skill's installed path instead of assuming it exists in the project.

Do not claim a cloud run succeeded without the required credentials and runtime.
Report skipped checks and unresolved placeholders explicitly.

## Placement Contract

The following locations are mandatory unless the framework itself is changed:

| Content | Location |
| --- | --- |
| Raw/file ingestion jobs | `etl/_flatfile/` |
| Bronze jobs | `etl/0_bronze/` |
| Silver jobs | `etl/1_silver/` |
| Gold jobs | `etl/2_gold/` |
| Diamond and ML jobs | `etl/3_diamond/` |
| Custom layer jobs | `etl/<custom-layer>/` |
| Shared transforms | `macros/` |
| Production model code | `models/` |
| ML notebooks | `model_notebooks/` |
| Generated ETL notebook | `outputs/notebook/` |
| Generated artifacts | `outputs/` |
| Runtime configuration | `configs/etl_config.yaml` |
| Spark profile configuration | `configs/spark/<profile>.toml` |

The current framework does not use a generic top-level `notebooks/` directory.
Do not invent one for generated ETL or ML notebooks. The ETL notebook generator
is currently Spark-oriented; do not generate a misleading ETL notebook for a
Pandas or Polars project unless the current generator has been made
engine-aware. ML notebooks scaffolded by `bolt init` are separate and are
engine-aware.

## Preference Resolution

Honor the user's preferences for:

- business names and descriptions
- source formats and locations
- engine and storage choices
- layer names when they still describe the dependency order
- scheduling and orchestration
- partitioning and incremental windows
- quality thresholds
- documentation, notebook, Airflow, and vendoring choices

The following framework constraints take precedence over convenience:

- `etl_config.yaml` is the runtime source of truth.
- Job modules must expose `process_data(self, input_tables)`.
- Dependencies must be expressed through `input_tables`.
- Reusable code belongs in `macros/`.
- Generated output belongs under `outputs/`.
- Secrets must stay in the environment or a secret manager.
