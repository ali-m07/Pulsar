from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, validator


class RedisNode(BaseModel):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False

    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        scheme = "rediss" if self.ssl else "redis"
        return f"{scheme}://{auth}{self.host}:{self.port}/{self.db}"


class RedisConfig(BaseModel):
    raw_master: RedisNode
    raw_slave: RedisNode
    prepared_master: RedisNode
    prepared_slave: RedisNode


class RabbitQueueNames(BaseModel):
    mru: str = "kandoo.mru"
    lru: str = "kandoo.lru"
    canary_mru: str = "kandoo.canary.mru"
    canary_lru: str = "kandoo.canary.lru"


class RabbitConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5672
    user: str = "guest"
    password: str = "guest"
    vhost: str = "/"
    queues: RabbitQueueNames = Field(default_factory=RabbitQueueNames)

    def pika_credentials(self):
        import pika

        return pika.PlainCredentials(self.user, self.password)


class ForecastConfig(BaseModel):
    horizons: List[int] = Field(default_factory=lambda: [30, 60, 90])
    min_history_points: int = 8
    max_history_points: int = 96
    city_ids: List[int] = Field(default_factory=lambda: [1])
    service_types: List[int] = Field(default_factory=lambda: [1, 2, 3])

    @validator("horizons", each_item=True)
    def _positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("horizon values must be positive minutes")
        return value


class PulsarConfig(BaseModel):
    redis: RedisConfig
    rabbitmq: RabbitConfig
    forecast: ForecastConfig = Field(default_factory=ForecastConfig)
    cache_dir: Path = Path("cache")
    period_duration_minutes: float = 7.5
    collect_duration_minutes: float = 5.5
    mlflow_tracking_uri: Optional[str] = None
    mlflow_experiment: str = "pulsar-forecast"

    def ensure_cache_dir(self) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir


def load_config(path: Path | str) -> PulsarConfig:
    config_path = Path(path)
    data = yaml.safe_load(config_path.read_text())
    return PulsarConfig.parse_obj(data)

