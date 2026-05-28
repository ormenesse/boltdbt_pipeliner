# demo_pandas - NYC collision risk model

Pandas translation of the NYC Open Data collision example.

Sources:
- Crashes: https://data.cityofnewyork.us/resource/h9gi-nx95.csv
- Vehicles: https://data.cityofnewyork.us/resource/bm4k-52h4.csv
- Persons: https://data.cityofnewyork.us/resource/f55k-p6yu.csv

The three files join on `collision_id`. The diamond layer trains a lightweight logistic model implemented with NumPy and writes quality metrics.

## Run

```bash
cd examples/demo_pandas
python scripts/download_nyc_collision_data.py --limit 2500
bolt run --config configs/etl_config.yaml
bolt test --config configs/etl_config.yaml
```

Layer outputs land in `data/layers/`, and model artifacts land in `outputs/models/collision_injury_logreg_pandas/`.
