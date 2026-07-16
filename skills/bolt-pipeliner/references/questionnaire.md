# Bolt Pipeliner Setup Questionnaire

Ask these questions in stages. Skip anything already answered by the project,
the environment profile, or the user's request.

## Core Questions

1. Is this a new project or an adaptation of an existing project? What is the
   project root and preferred project name?
2. What business outcome should the pipeline produce? Which final tables,
   features, metrics, or model artifacts are expected?
3. What are the input sources? Ask for source names, formats, schemas or sample
   files, refresh behavior, and locations. Distinguish local files, shared
   catalog tables, APIs, and databases.
4. Which processing engine should be the default: PySpark, Pandas, or Polars?
   If needed, ask whether different jobs use different engines.
5. Which storage format and runtime are required? For Spark, distinguish
   Iceberg, Delta, and Parquet where relevant.
6. Which layers should exist and in what dependency order? Offer flatfile,
   bronze, silver, gold, and diamond, but allow meaningful custom names below
   `etl/`.
7. Where will the pipeline run: terminal, notebook, Airflow, Databricks Jobs,
   or another orchestrator? For Spark, ask for the profile such as local,
   Databricks, EMR, Glue, GCP, Azure, or Kubernetes.
8. Where should flatfiles and transformed datasets live? Ask for catalog and
   schema names. Do not ask the user to paste credentials.
9. Is processing incremental? If yes, ask for the incremental column, type,
   date grain, append/overwrite/window behavior, and partition columns.
10. Which data-quality checks are required? Ask about nullability, uniqueness,
    row counts, freshness, accepted values, and cross-table expectations.

## Conditional Questions

- Ask about Airflow schedules, retries, connection IDs, cluster names, images,
  and application settings when Airflow generation is requested. Record
  placeholders or environment-variable names instead of secrets.
- Ask about model training, prediction tables, MLflow, model registry, and
  experiment notebooks when ML is requested.
- Ask whether generated documentation, layer scripts, Airflow DAGs, and ETL
  notebooks should be produced.
- Ask whether the project should vendor Bolt Pipeliner or depend on a package
  installation.
- Ask about naming, formatting, dependency-management, and test conventions
  only when the existing repository does not establish them.

## Confirmation Summary

Before editing, summarize the answers as:

```text
Project: <name> at <root>
Mode: new | existing
Engine/storage: <engine> / <format>
Layers: <ordered layers and paths>
Runtime: <execution environment and profile>
Data: <input and output locations>
Incremental: <policy or disabled>
Quality: <checks>
Artifacts: <requested generated outputs>
ML: enabled | disabled
Vendor: enabled | disabled
```

Ask the user to confirm this summary. If a required value is unknown, mark it
as unresolved rather than inventing a production value.

## Persistence

After confirmation, write or update `configs/bolt_environment.yaml` with the
non-secret decisions. Keep executable job definitions and dependencies in
`configs/etl_config.yaml`; do not duplicate the job graph in the profile.
