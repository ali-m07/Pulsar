from __future__ import annotations

from dataclasses import dataclass
from typing import List

import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

from ..config import PulsarConfig
from ..store import TimeSeriesStore


@dataclass
class TrainingResult:
    hexagons: int
    rows: int
    mae: float
    rmse: float
    model_uri: str


class MLTrainer:
    """Train a regression model on historical demand signals and log it to MLflow."""

    def __init__(self, cfg: PulsarConfig):
        self.cfg = cfg
        self.store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")

    def _load_dataset(self, service_types: List[int]) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        cache_root = self.store.root
        for parquet_path in cache_root.glob("*.parquet"):
            parts = parquet_path.stem.split("_")
            if len(parts) != 2:
                continue
            hexagon = int(parts[0])
            service_type = int(parts[1])
            if service_types and service_type not in service_types:
                continue
            df = pd.read_parquet(parquet_path)
            df["hexagon"] = hexagon
            df["service_type"] = service_type
            frames.append(df)
        if not frames:
            raise ValueError("no parquet history found; run sync first")
        frame = pd.concat(frames, ignore_index=True)
        frame["period_start"] = pd.to_datetime(frame["period_start"], errors="coerce")
        frame.sort_values(["hexagon", "service_type", "period_start"], inplace=True)
        frame["lag_demand"] = frame.groupby(["hexagon", "service_type"])["demand_signal"].shift(1)
        frame["lag_supply"] = frame.groupby(["hexagon", "service_type"])["supply_signal"].shift(1)
        frame["lag_acceptance"] = frame.groupby(["hexagon", "service_type"])["acceptance_rate"].shift(1)
        frame.dropna(
            subset=["lag_demand", "lag_supply", "lag_acceptance", "price_conversion", "demand_signal"],
            inplace=True,
        )
        return frame

    def train(self, service_types: List[int], alpha: float = 0.3, l1_ratio: float = 0.1) -> TrainingResult:
        dataset = self._load_dataset(service_types)
        features = dataset[
            ["lag_demand", "lag_supply", "lag_acceptance", "price_conversion"]
        ].astype(float)
        target = dataset["demand_signal"].astype(float)
        x_train, x_test, y_train, y_test = train_test_split(features, target, test_size=0.2, shuffle=True)

        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        model.fit(x_train, y_train)

        preds = model.predict(x_test)
        mae = float(mean_absolute_error(y_test, preds))
        rmse = float(np.sqrt(mean_squared_error(y_test, preds)))

        if self.cfg.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.cfg.mlflow_tracking_uri)
        mlflow.set_experiment(self.cfg.mlflow_experiment)
        with mlflow.start_run(run_name="elasticnet-demand"):
            mlflow.log_param("alpha", alpha)
            mlflow.log_param("l1_ratio", l1_ratio)
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("rmse", rmse)
            model_info = mlflow.sklearn.log_model(model, artifact_path="model")
            model_uri = model_info.model_uri

        return TrainingResult(
            hexagons=dataset["hexagon"].nunique(),
            rows=len(dataset),
            mae=mae,
            rmse=rmse,
            model_uri=model_uri,
        )

