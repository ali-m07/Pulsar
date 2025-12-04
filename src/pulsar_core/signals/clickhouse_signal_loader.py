from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd

from ..periods import PeriodWindow
from .clickhouse_loader import ClickHouseLoader

logger = logging.getLogger(__name__)


class ClickHouseSignalLoader:
    """
    Signal loader that reads from ClickHouse kandoo_parameter_nats table.
    This provides the same interface as RedisSignalLoader but uses ClickHouse data.
    """

    def __init__(self, clickhouse_loader: ClickHouseLoader):
        self.ch_loader = clickhouse_loader
        self._cache: Dict[str, Dict] = {}

    def _get_cache_key(self, period: PeriodWindow, hexagon: int, service_type: int) -> str:
        """Generate cache key for period/hexagon/service_type combination."""
        return f"{period.start.isoformat()}_{hexagon}_{service_type}"

    def _load_period_data(self, period: PeriodWindow, hexagon: int, service_type: int) -> Optional[Dict]:
        """Load data for a specific period from ClickHouse."""
        cache_key = self._get_cache_key(period, hexagon, service_type)
        
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            # Query ClickHouse for this specific period
            df = self.ch_loader.load_parameters(
                from_date=period.start,
                to_date=period.end,
                hexagons=[hexagon],
                service_types=[service_type],
            )
            
            if df.empty:
                return None
            
            # Get the first row (should be only one for this period/hexagon/service_type)
            row = df.iloc[0]
            
            data = {
                "acceptance_rate": float(row.get("accept_rate", 0.0)),
                "price_conversion": float(row.get("price_cnvr", 0.0)),
                "surge_percent": int(row.get("surge_percent", 0)),
                "surge_absolute": int(row.get("surge_absolute", 0)),
                "cumulative_surge_percent": int(row.get("cumulative_surge_percent", 0)),
                "cumulative_surge_absolute": int(row.get("cumulative_surge_absolute", 0)),
                "rule_sheet_id": int(row.get("rule_sheet_id", 0)) if pd.notna(row.get("rule_sheet_id")) else None,
            }
            
            # Calculate demand and supply signals from the data
            # These are approximations based on available metrics
            # In real implementation, you might need to query additional tables
            data["demand_signal"] = float(row.get("price_cnvr", 0.0)) * 100  # Approximation
            data["supply_signal"] = float(row.get("accept_rate", 0.0)) * 100  # Approximation
            
            self._cache[cache_key] = data
            return data
        except Exception as exc:
            logger.error(f"Failed to load period data from ClickHouse: {exc}", exc_info=True)
            return None

    def acceptance_metrics(self, period: PeriodWindow, hexagon: int, service_type: int) -> Dict[str, int]:
        """Get acceptance rate metrics (compatible with RedisSignalLoader interface)."""
        data = self._load_period_data(period, hexagon, service_type)
        if not data:
            return {"requests": 0, "accepts": 0}
        
        # Approximate requests and accepts from acceptance_rate
        # This is a simplification - in production you might need actual counts
        acceptance_rate = data.get("acceptance_rate", 0.0)
        # Assume base of 100 requests for calculation
        accepts = int(acceptance_rate * 100) if acceptance_rate > 0 else 0
        requests = 100 if accepts > 0 else 0
        
        return {"requests": requests, "accepts": accepts}

    def price_metrics(
        self, period: PeriodWindow, hexagon: int, service_type: int, service_category: int
    ) -> Dict[str, int]:
        """Get price conversion metrics (compatible with RedisSignalLoader interface)."""
        data = self._load_period_data(period, hexagon, service_type)
        if not data:
            return {"ride_requests": 0, "get_prices": 0}
        
        # Approximate from price_conversion
        price_conversion = data.get("price_conversion", 0.0)
        # Assume base of 100 get_prices for calculation
        ride_requests = int(price_conversion * 100) if price_conversion > 0 else 0
        get_prices = 100 if ride_requests > 0 else 0
        
        return {"ride_requests": ride_requests, "get_prices": get_prices}

    def surge_metrics(self, period: PeriodWindow, hexagon: int, service_type: int) -> Dict[str, Optional[float]]:
        """Get surge metrics from ClickHouse data."""
        data = self._load_period_data(period, hexagon, service_type)
        if not data:
            return {
                "surge_percent": None,
                "surge_absolute": None,
                "cumulative_surge_percent": None,
                "cumulative_surge_absolute": None,
                "rule_sheet_id": None,
            }
        
        return {
            "surge_percent": float(data.get("surge_percent", 0)),
            "surge_absolute": float(data.get("surge_absolute", 0)),
            "cumulative_surge_percent": float(data.get("cumulative_surge_percent", 0)),
            "cumulative_surge_absolute": float(data.get("cumulative_surge_absolute", 0)),
            "rule_sheet_id": data.get("rule_sheet_id"),
        }

