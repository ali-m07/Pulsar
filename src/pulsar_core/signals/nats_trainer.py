from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import nats
from nats.aio.client import Client as NATS

import pandas as pd

from ..config import PulsarConfig
from ..models.cnn_trainer import CNNTrainer, CNNTrainerConfig
from ..models.trainer import MLTrainer
from ..signals.csv_loader import CSVSignalLoader, load_csv_files
from ..signals.import_task import ImportTask
from ..features import SnapshotBuilder
from ..store import TimeSeriesStore

logger = logging.getLogger(__name__)


class NATSTrainerConsumer:
    """NATS consumer for online training events."""

    def __init__(self, cfg: PulsarConfig, data_dir: Path):
        if not cfg.nats:
            raise ValueError("NATS config is required for NATS trainer")
        self.cfg = cfg
        self.data_dir = data_dir
        self._nc: Optional[NATS] = None
        self._sub = None
        
        # Load CSV data once
        logger.info(f"Loading CSV files from {data_dir}")
        self.csv_data = load_csv_files(data_dir)
        logger.info(f"Loaded {len(self.csv_data)} rows from CSV files")
        
        # Build runtime components
        loader = CSVSignalLoader(self.csv_data)
        self.builder = SnapshotBuilder(cfg, loader)
        self.store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")

    async def _connect(self) -> None:
        if self._nc and self._nc.is_connected:
            return
        
        options = {
            "servers": self.cfg.nats.servers,
            "max_reconnect_attempts": self.cfg.nats.max_reconnects,
            "reconnect_time_wait": self.cfg.nats.reconnect_time_wait,
        }
        
        self._nc = await nats.connect(**options)
        logger.info(f"Connected to NATS servers: {self.cfg.nats.servers}")

    async def _handle_training_event(self, msg) -> None:
        """Handle training event from NATS."""
        try:
            data = msg.data.decode("utf-8")
            payload = json.loads(data)
            
            logger.info(f"Received training event: {payload}")
            
            # Extract training parameters from payload
            model_type = payload.get("model_type", "elasticnet")
            service_types = payload.get("service_types", [])
            alpha = payload.get("alpha", 0.3)
            l1_ratio = payload.get("l1_ratio", 0.1)
            train_date_str = payload.get("train_date")
            force = payload.get("force", False)
            
            # Parse train date
            train_date = None
            if train_date_str:
                train_date = date.fromisoformat(train_date_str)
            
            # Process CSV data and build snapshots
            await self._process_csv_data()
            
            # Train model
            await self._train_model(
                model_type=model_type,
                service_types=service_types,
                alpha=alpha,
                l1_ratio=l1_ratio,
                train_date=train_date,
                force=force,
            )
            
            logger.info("Training completed successfully")
            
        except Exception as exc:
            logger.error(f"Error handling training event: {exc}", exc_info=True)

    async def _process_csv_data(self) -> None:
        """Process CSV data and build snapshots."""
        logger.info("Processing CSV data and building snapshots...")
        
        from ..periods import PeriodWindow, with_duration
        
        # Group by periods and create tasks
        csv_data = self.csv_data.sort_values('clickhouse_time')
        csv_data['period_start'] = csv_data['clickhouse_time'].apply(
            lambda ts: with_duration(
                ts,
                self.cfg.period_duration_minutes,
                self.cfg.collect_duration_minutes
            ).start
        )
        
        processed = 0
        for (period_start, service_type, city_id), group in csv_data.groupby(
            ['period_start', 'service_type', 'city_id']
        ):
            for hex_id in group['hex_id'].unique():
                period_end = period_start + pd.Timedelta(minutes=self.cfg.period_duration_minutes)
                collect_end = period_start + pd.Timedelta(minutes=self.cfg.collect_duration_minutes)
                
                period = PeriodWindow(
                    start=period_start,
                    end=period_end,
                    duration=period_end - period_start,
                    collect_duration=collect_end - period_start,
                )
                
                task = ImportTask(
                    hexagon=int(hex_id),
                    service_type=int(service_type),
                    city_id=int(city_id),
                    period=period,
                    expansion_weight=None,
                )
                
                snapshot = self.builder.build(task)
                self.store.append(snapshot)
                processed += 1
        
        logger.info(f"Processed {processed} snapshots from CSV data")

    async def _train_model(
        self,
        model_type: str,
        service_types: list[int],
        alpha: float,
        l1_ratio: float,
        train_date: Optional[date],
        force: bool,
    ) -> None:
        """Train the model."""
        logger.info(f"Training {model_type} model...")
        
        if model_type == "cnn":
            tcfg = CNNTrainerConfig(
                window_size=12,
                epochs=25,
                batch_size=128,
            )
            trainer = CNNTrainer(self.cfg, tcfg)
            result = trainer.train(service_types=service_types)
        else:
            trainer = MLTrainer(self.cfg)
            result = trainer.train(
                service_types=service_types,
                alpha=alpha,
                l1_ratio=l1_ratio,
                train_date=train_date,
                force=force,
            )
        
        logger.info(
            f"Training completed: {result.rows} rows ({result.hexagons} hexagons). "
            f"MAE={result.mae:.2f}, RMSE={result.rmse:.2f}, model_uri={result.model_uri}"
        )

    async def start(self) -> None:
        """Start consuming training events from NATS."""
        await self._connect()
        assert self._nc is not None
        
        # Subscribe to training subject
        training_subject = self.cfg.nats.subject.replace("parameter", "train")
        if self.cfg.nats.queue:
            self._sub = await self._nc.subscribe(
                training_subject,
                queue=self.cfg.nats.queue,
                cb=self._handle_training_event,
            )
            logger.info(f"Subscribed to {training_subject} with queue {self.cfg.nats.queue}")
        else:
            self._sub = await self._nc.subscribe(
                training_subject,
                cb=self._handle_training_event,
            )
            logger.info(f"Subscribed to {training_subject}")

    async def stop(self) -> None:
        """Stop consuming and close connection."""
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc:
            await self._nc.close()
            logger.info("NATS connection closed")


async def run_nats_trainer(cfg: PulsarConfig, data_dir: Path) -> None:
    """Run NATS trainer consumer."""
    consumer = NATSTrainerConsumer(cfg, data_dir)
    try:
        await consumer.start()
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await consumer.stop()

