from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Optional

import aio_pika

from ..config import PulsarConfig
from .import_task import ImportTask

ImportHandler = Callable[[ImportTask], Awaitable[None]]


class ImportTaskConsumer:
    """Asynchronous RabbitMQ consumer for Scheduler -> Worker traffic."""

    def __init__(self, cfg: PulsarConfig, handler: ImportHandler, canary: bool = False):
        self.cfg = cfg
        self.handler = handler
        queues = cfg.rabbitmq.queues
        self.queue_name = queues.canary_mru if canary else queues.mru
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.RobustChannel] = None

    async def _connect(self) -> None:
        if self._connection:
            return
        self._connection = await aio_pika.connect_robust(
            host=self.cfg.rabbitmq.host,
            port=self.cfg.rabbitmq.port,
            login=self.cfg.rabbitmq.user,
            password=self.cfg.rabbitmq.password,
            virtualhost=self.cfg.rabbitmq.vhost,
        )
        self._channel = await self._connection.channel()

    async def start(self) -> None:
        await self._connect()
        assert self._channel is not None
        queue = await self._channel.declare_queue(self.queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        payload = json.loads(message.body.decode("utf-8"))
                        task = ImportTask.from_payload(payload)
                    except Exception as exc:  # pylint: disable=broad-except
                        print(f"[pulsar] failed to parse import task: {exc}")
                        continue
                    await self.handler(task)

    async def stop(self) -> None:
        if self._channel:
            await self._channel.close()
        if self._connection:
            await self._connection.close()


async def run_consumer(cfg: PulsarConfig, handler: ImportHandler, canary: bool = False) -> None:
    consumer = ImportTaskConsumer(cfg, handler, canary=canary)
    try:
        await consumer.start()
    finally:
        await consumer.stop()


def run_until_cancelled(cfg: PulsarConfig, handler: ImportHandler, canary: bool = False) -> None:
    asyncio.run(run_consumer(cfg, handler, canary))

