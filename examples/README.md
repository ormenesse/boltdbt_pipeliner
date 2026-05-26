# Examples

Reference projects you can `cd` into to see `bolt_pipeliner` configured for real workloads. The framework code itself lives at the repo root under `src/bolt_pipeliner/`; everything in this folder is **only here as a reference**.

| Example | Engine(s) | Architecture | Notes |
|---|---|---|---|
| [`demo/`](./demo/) | PySpark + Iceberg | flatfile → bronze → silver → gold | The minimal, runnable repo demo. Four flatfile jobs ingest CSVs and one silver job builds a monthly fact table with `tests:` declared. |
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

(Most examples reference S3 paths and require real credentials to actually execute the data loads. They're best read as *config + job-module patterns* you can copy into your own project.)

## Starting fresh

If you want a clean scaffold rather than adapting one of these, run:

```bash
bolt init my_project --preset medallion
```

Available presets: `minimal`, `medallion`, `diamond`, `pandas`, `polars`. See the root [`README.md`](../README.md#quick-start--bolt-init) for the full guide.
