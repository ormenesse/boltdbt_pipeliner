from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl


CATEGORICAL_FEATURES = [
    "borough",
    "day_of_week",
    "primary_factor",
    "primary_vehicle_type",
    "secondary_vehicle_type",
]

NUMERIC_FEATURES = [
    "crash_month",
    "crash_hour",
    "is_weekend",
    "is_night",
    "location_known",
    "vehicle_count",
    "passenger_car_count",
    "suv_wagon_count",
    "truck_van_count",
    "taxi_count",
    "bus_count",
    "two_wheeler_count",
    "avg_vehicle_age",
    "max_vehicle_occupants",
    "property_damage_vehicle_count",
    "driver_inattention_count",
    "unsafe_speed_count",
    "failure_to_yield_count",
    "impairment_count",
    "person_count",
    "occupant_count",
    "pedestrian_person_count",
    "cyclist_person_count",
    "minor_count",
    "senior_count",
    "avg_person_age",
    "female_person_count",
    "male_person_count",
    "safety_equipment_count",
    "factor_driver_inattention",
    "factor_speeding",
    "factor_yield",
    "factor_impairment",
]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -30.0, 30.0)))


def _train_logistic(x: np.ndarray, y: np.ndarray, iterations: int = 400, lr: float = 0.15) -> np.ndarray:
    design = np.concatenate([np.ones((x.shape[0], 1)), x], axis=1)
    weights = np.zeros(design.shape[1], dtype=float)
    for _ in range(iterations):
        probs = _sigmoid(design @ weights)
        grad = (design.T @ (probs - y)) / y.size
        weights -= lr * grad
    return weights


def _roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    positives = y_true == 1.0
    negatives = y_true == 0.0
    n_pos = int(positives.sum())
    n_neg = int(negatives.sum())
    if n_pos == 0 or n_neg == 0:
        return 0.0

    order = np.argsort(y_score)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, y_score.size + 1)
    sum_pos = float(ranks[positives].sum())
    return (sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def _metrics_df(
    status: str,
    row_count: int,
    feature_columns: str,
    train_count: int = 0,
    test_count: int = 0,
    auc: float = 0.0,
    accuracy: float = 0.0,
    positive_rate: float = 0.0,
    model_path: str = "",
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "model_name": ["collision_injury_logistic_regression_polars"],
            "status": [status],
            "row_count": [int(row_count)],
            "train_count": [int(train_count)],
            "test_count": [int(test_count)],
            "auc": [float(auc)],
            "accuracy": [float(accuracy)],
            "positive_rate": [float(positive_rate)],
            "feature_columns": [feature_columns],
            "model_path": [model_path],
            "trained_at": [datetime.now(timezone.utc).isoformat()],
        }
    )


def process_data(self, input_tables):
    training = input_tables["training"].with_columns(pl.col("label").cast(pl.Float64).fill_null(0.0))
    row_count = training.height
    if row_count < 20:
        return _metrics_df("skipped_too_few_rows", row_count, feature_columns="")

    positive_rate = float(training["label"].mean())
    if training.select(pl.col("label").n_unique()).item() < 2:
        return _metrics_df(
            "skipped_single_class",
            row_count,
            feature_columns=",".join(CATEGORICAL_FEATURES + NUMERIC_FEATURES),
            positive_rate=positive_rate,
        )

    feature_source = training.with_columns(
        *[pl.col(col).cast(pl.Utf8).fill_null("UNKNOWN").alias(col) for col in CATEGORICAL_FEATURES],
        *[pl.col(col).cast(pl.Float64).fill_null(0.0).alias(col) for col in NUMERIC_FEATURES],
    )

    categorical = feature_source.select(CATEGORICAL_FEATURES).to_dummies(separator="__")
    numeric = feature_source.select(NUMERIC_FEATURES)
    feature_df = pl.concat([numeric, categorical], how="horizontal")

    x = feature_df.to_numpy()
    y = feature_source["label"].to_numpy()

    rng = np.random.default_rng(42)
    idx = np.arange(row_count)
    rng.shuffle(idx)
    split = max(1, int(row_count * 0.8))
    split = min(split, row_count - 1)
    train_idx = idx[:split]
    test_idx = idx[split:]
    if test_idx.size == 0:
        test_idx = train_idx

    weights = _train_logistic(x[train_idx], y[train_idx])
    test_design = np.concatenate([np.ones((test_idx.size, 1)), x[test_idx]], axis=1)
    probs = _sigmoid(test_design @ weights)
    preds = (probs >= 0.5).astype(float)
    auc = _roc_auc(y[test_idx], probs)
    accuracy = float((preds == y[test_idx]).mean())

    model_dir = Path(self.bucket).parent / "models" / "collision_injury_logreg_polars"
    if self.unload:
        model_dir.mkdir(parents=True, exist_ok=True)
        coef_df = pl.DataFrame(
            {
                "feature": ["intercept", *feature_df.columns],
                "coefficient": weights,
            }
        )
        coef_df.write_csv(model_dir / "coefficients.csv")

    return _metrics_df(
        "trained",
        row_count,
        feature_columns=",".join(feature_df.columns),
        train_count=train_idx.size,
        test_count=test_idx.size,
        auc=auc,
        accuracy=accuracy,
        positive_rate=positive_rate,
        model_path=str(model_dir),
    )
