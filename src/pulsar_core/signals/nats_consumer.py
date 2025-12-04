from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional

import nats
from nats.aio.client import Client as NATS

from ..config import PulsarConfig
from .import_task import ImportTask

logger = logging.getLogger(__name__)

ImportHandler = Callable[[ImportTask], Awaitable[None]]


class NATSConsumer:
    """NATS consumer for kandoo.parameter messages from Kafka bridge."""

    def __init__(self, cfg: PulsarConfig, handler: ImportHandler):
        if not cfg.nats:
            raise ValueError("NATS config is required for NATS consumer")
        self.cfg = cfg.nats
        self.handler = handler
        self._nc: Optional[NATS] = None
        self._sub = None

    async def _connect(self) -> None:
        if self._nc and self._nc.is_connected:
            return
        
        options = {
            "servers": self.cfg.servers,
            "max_reconnect_attempts": self.cfg.max_reconnects,
            "reconnect_time_wait": self.cfg.reconnect_time_wait,
        }
        
        self._nc = await nats.connect(**options)
        logger.info(f"Connected to NATS servers: {self.cfg.servers}")

    async def _message_handler(self, msg) -> None:
        """Handle incoming NATS message."""
        try:
            # Decode message
            data = msg.data.decode("utf-8")
            payload = json.loads(data)
            
            # Convert kandoo.parameter message to ImportTask
            # The payload structure from ClickHouse kandoo_parameter_nats table
            task = self._parse_kandoo_parameter(payload)
            if task:
                await self.handler(task)
            else:
                logger.warning(f"Could not parse message: {payload}")
        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as exc:
            logger.error(f"Failed to parse NATS message: {exc}", exc_info=True)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"Error handling NATS message: {exc}", exc_info=True)

    def _parse_kandoo_parameter(self, payload: dict) -> Optional[ImportTask]:
        """Parse kandoo.parameter message into ImportTask."""
        try:
            from datetime import datetime
            from ..periods import PeriodWindow
            
            # Extract period from 'from' and 'to' fields
            period_from = datetime.fromisoformat(payload["from"].replace("Z", "+00:00"))
            period_to = datetime.fromisoformat(payload["to"].replace("Z", "+00:00"))
            
            period = PeriodWindow(
                start=period_from,
                end=period_to,
                duration=period_to - period_from,
                collect_duration=period_to - period_from,  # Default to full period
            )
            
            # Create ImportTask from kandoo parameter data
            task = ImportTask(
                hexagon=int(payload["hex_id"]),
                service_type=int(payload["service_type"]),
                city_id=int(payload.get("city_id", 0)),
                period=period,
                expansion_weight=None,  # Not available in kandoo.parameter
            )
            return task
        except (KeyError, ValueError, TypeError) as exc:
            logger.error(f"Failed to parse kandoo.parameter: {exc}")
            return None

    async def start(self) -> None:
        """Start consuming messages from NATS."""
        await self._connect()
        assert self._nc is not None
        
        # Subscribe to subject
        if self.cfg.queue:
            self._sub = await self._nc.subscribe(
                self.cfg.subject,
                queue=self.cfg.queue,
                cb=self._message_handler,
            )
            logger.info(f"Subscribed to {self.cfg.subject} with queue {self.cfg.queue}")
        else:
            self._sub = await self._nc.subscribe(
                self.cfg.subject,
                cb=self._message_handler,
            )
            logger.info(f"Subscribed to {self.cfg.subject}")

    async def stop(self) -> None:
        """Stop consuming and close connection."""
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc:
            await self._nc.close()
            logger.info("NATS connection closed")


async def run_nats_consumer(cfg: PulsarConfig, handler: ImportHandler) -> None:
    """Run NATS consumer with proper error handling."""
    consumer = NATSConsumer(cfg, handler)
    try:
        await consumer.start()
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await consumer.stop()

