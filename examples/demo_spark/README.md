# demo_spark - NYC collision risk model

Real public-data Spark demo using NYC Open Data motor-vehicle collision CSVs.

Sources:
- Crashes: https://data.cityofnewyork.us/resource/h9gi-nx95.csv
- Vehicles: https://data.cityofnewyork.us/resource/bm4k-52h4.csv
- Persons: https://data.cityofnewyork.us/resource/f55k-p6yu.csv

The three files join on `collision_id`. The diamond layer trains a PySpark ML logistic-regression model that predicts whether a collision had at least one injured person.

## Run

```bash
cd examples/demo_spark
python scripts/download_nyc_collision_data.py --limit 2500
bolt run --config configs/etl_config.yaml
```

Use `bolt run --config configs/etl_config.yaml --verbose` to print each job/module as it executes.

The example config uses generic incremental policy keys (root defaults plus per-job overrides):
`incremental_column`, `incremental_type`, `incremental_unit`, and `incremental_date_grain`.

`bolt run` automatically loads `configs/spark/local.toml`, including a deliberately small `spark.sql.shuffle.partitions` value so the demo shows where Spark tuning belongs.

Layer outputs land in `data/layers/` and the trained model lands in `outputs/models/collision_injury_logreg/`.
