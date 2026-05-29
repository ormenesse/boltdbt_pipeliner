# Examples

Reference projects you can `cd` into to see `bolt_pipeliner` configured for real workloads. The framework code itself lives at the repo root under `src/bolt_pipeliner/`; everything in this folder is **only here as a reference**.

| Example | Engine(s) | Architecture | Notes |
|---|---|---|---|
| [`demo/`](./demo/) | PySpark + Iceberg | flatfile → bronze → silver → gold | The minimal, runnable repo demo. Four flatfile jobs ingest CSVs and one silver job builds a monthly fact table with `tests:` declared. |
| [`demo_spark/`](./demo_spark/) | PySpark + local Parquet | flatfile → bronze → silver → gold → diamond | Real NYC Open Data collision CSVs joined on `collision_id`; the diamond layer trains a Spark ML injury-risk model. |
| [`demo_pandas/`](./demo_pandas/) | Pandas + local Parquet | flatfile → bronze → silver → gold → diamond | Same NYC collision scenario translated to Pandas; the diamond layer trains a NumPy logistic model. |
| [`demo_polars/`](./demo_polars/) | Polars + local Parquet | flatfile → bronze → silver → gold → diamond | Same NYC collision scenario translated to Polars; the diamond layer trains a NumPy logistic model from Polars features. |
| [`entergy/`](./entergy/) | PySpark + Iceberg (Glue) | medallion (bronze/silver/domain) | Production-grade reference — 100+ jobs, full Airflow generation, HTML docs. Useful for seeing patterns at scale. |
| [`peco/`](./peco/) | PySpark, Pandas, Polars (Parquet) | bronze/silver/gold | Multi-engine reference. Demonstrates how a single project can mix `ETLBaseParquet`, `ETLBaseParquetPandas`, and `ETLBaseParquetPolars` via per-job `class_name:`. |

## How to use these

Every example follows the same convention:

```bash
cd examples/<name>
pip install -e ../..              # install the framework in editable mode
bolt run --config configs/etl_config.yaml --silver
bolt test --config configs/etl_config.yaml
bolt generate documentation
```

Add `--verbose` to `bolt run` when you want per-job execution logs.

All maintained demos now use the generalized incremental config contract:
`incremental_column`, `incremental_type` (`int`/`date`), `incremental_unit`
(`-1`/`overwrite`, `0`/`append`, `N > 0` window), and `incremental_date_grain`
(`yearly`/`monthly`/`daily` for date mode).

(Some examples (for example `demo_spark/`, `demo_pandas/`, and `demo_polars/`) run fully local. Larger reference projects may reference S3 paths and require real credentials.)

The local demos use `configs.output_location: data/layers` and `configs.flatfile_location: data/raw`.

## Starting fresh

If you want a clean scaffold rather than adapting one of these, run:

```bash
bolt init my_project --preset medallion
```

Available presets: `minimal`, `medallion`, `diamond`, `pandas`, `polars`. See the root [`README.md`](../README.md#quick-start--bolt-init) for the full guide.
