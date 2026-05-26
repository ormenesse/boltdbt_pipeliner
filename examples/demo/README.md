# demo — PySpark + Iceberg medallion

The canonical minimal pipeline used to validate `bolt_pipeliner` end-to-end.

## Layout

```
configs/
  etl_config.yaml      # 4 flatfile jobs + 1 silver job
  style_config.yaml    # colors for `bolt generate documentation`
etl/
  _flatfile/           # 4 jobs: storm events, CPI regional, gas prices, digital sessions
  0_bronze/            # empty placeholder layer
  1_silver/            # 1 job: fct_state_gas_cpi_monthly (uses macros + window functions)
  2_gold/              # empty placeholder layer
```

## What it shows

- A single `silver` job aggregating two upstream flatfile inputs (`cpi`, `gas`) into a monthly fact table.
- `incremental: true` with `partition_by: [year_month]` — the Iceberg base only rewrites the last three monthly partitions on each run.
- A `tests:` block on the silver job (`not_null`, `unique`, `row_count`, `freshness`) — `bolt test` runs them after `process_data`.
- A `class_name: ETLBase` declaration on every job — the default Spark+Iceberg base.

## Running it

```bash
cd examples/demo
bolt run --silver --config configs/etl_config.yaml      # needs a working Spark + Iceberg
bolt test --config configs/etl_config.yaml              # runs the tests: block
bolt generate documentation --config configs/etl_config.yaml
bolt generate airflow --config configs/etl_config.yaml
```

Output artifacts land under `examples/demo/outputs/` (gitignored).

## Adapting it

Treat this folder as the seed for a new project: copy it, rename layers in `configs/etl_config.yaml`, swap out the job modules, and adjust `class_name` if you want a non-Spark engine (`ETLBaseParquetPandas`, `ETLBaseParquetPolars`, …).
