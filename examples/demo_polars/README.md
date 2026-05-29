# demo_polars - NYC collision risk model

Polars translation of the NYC Open Data collision example.

Sources:
- Crashes: https://data.cityofnewyork.us/resource/h9gi-nx95.csv
- Vehicles: https://data.cityofnewyork.us/resource/bm4k-52h4.csv
- Persons: https://data.cityofnewyork.us/resource/f55k-p6yu.csv

The three files join on `collision_id`. The diamond layer trains a lightweight logistic model from Polars features and writes quality metrics.

## Run

```bash
cd examples/demo_polars
python scripts/download_nyc_collision_data.py --limit 2500
bolt run --config configs/etl_config.yaml
bolt test --config configs/etl_config.yaml
```

Use `bolt run --config configs/etl_config.yaml --verbose` to print each job/module as it executes.

The example config uses generic incremental policy keys (root defaults plus per-job overrides):
`incremental_column`, `incremental_type`, `incremental_unit`, and `incremental_date_grain`.

Layer outputs land in `data/layers/`, and model artifacts land in `outputs/models/collision_injury_logreg_polars/`.
