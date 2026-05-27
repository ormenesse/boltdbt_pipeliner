from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import Row, functions as F


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


def _metrics_df(self, status, row_count, train_count=0, test_count=0, auc=0.0, accuracy=0.0, positive_rate=0.0, model_path=""):
    return self.spark.createDataFrame(
        [
            Row(
                model_name="collision_injury_logistic_regression",
                status=status,
                row_count=int(row_count),
                train_count=int(train_count),
                test_count=int(test_count),
                auc=float(auc),
                accuracy=float(accuracy),
                positive_rate=float(positive_rate),
                feature_columns=",".join(CATEGORICAL_FEATURES + NUMERIC_FEATURES),
                model_path=model_path,
                trained_at=datetime.now(timezone.utc).isoformat(),
            )
        ]
    )


def process_data(self, input_tables):
    training = input_tables["training"].dropna(subset=["label"])
    row_count = training.count()
    if row_count < 20:
        return _metrics_df(self, "skipped_too_few_rows", row_count)

    positive_rate = training.agg(F.avg("label").alias("positive_rate")).first()["positive_rate"] or 0.0
    if training.select("label").distinct().count() < 2:
        return _metrics_df(self, "skipped_single_class", row_count, positive_rate=positive_rate)

    indexed_cols = [f"{col}_idx" for col in CATEGORICAL_FEATURES]
    encoded_cols = [f"{col}_vec" for col in CATEGORICAL_FEATURES]
    indexers = [
        StringIndexer(inputCol=col, outputCol=indexed, handleInvalid="keep")
        for col, indexed in zip(CATEGORICAL_FEATURES, indexed_cols)
    ]
    encoder = OneHotEncoder(inputCols=indexed_cols, outputCols=encoded_cols)
    assembler = VectorAssembler(
        inputCols=encoded_cols + NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        regParam=0.05,
    )
    pipeline = Pipeline(stages=[*indexers, encoder, assembler, classifier])

    train_df, test_df = training.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    if test_count == 0:
        test_df = train_df
        test_count = train_count

    model = pipeline.fit(train_df)
    scored = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = evaluator.evaluate(scored)
    accuracy = scored.agg(
        F.avg((F.col("prediction") == F.col("label")).cast("double")).alias("accuracy")
    ).first()["accuracy"]

    model_path = Path(self.bucket).parent / "models" / "collision_injury_logreg"
    if self.unload:
        model.write().overwrite().save(str(model_path))

    return _metrics_df(
        self,
        "trained",
        row_count,
        train_count=train_count,
        test_count=test_count,
        auc=auc,
        accuracy=accuracy or 0.0,
        positive_rate=positive_rate,
        model_path=str(model_path),
    )
