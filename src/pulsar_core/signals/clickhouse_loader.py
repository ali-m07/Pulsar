from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Iterable, Optional

import clickhouse_connect
import pandas as pd

from ..config import PulsarConfig
from ..periods import PeriodWindow
from .import_task import ImportTask

logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Load kandoo parameter data from ClickHouse kandoo_parameter_nats table."""

    def __init__(self, cfg: PulsarConfig):
        if not cfg.clickhouse:
            raise ValueError("ClickHouse config is required")
        self.cfg = cfg.clickhouse
        self._client = None

    def _get_client(self):
        """Get or create ClickHouse client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.cfg.host,
                port=self.cfg.port,
                database=self.cfg.database,
                username=self.cfg.user,
                password=self.cfg.password,
                secure=self.cfg.secure,
            )
        return self._client

    def load_parameters(
        self,
        from_date: datetime,
        to_date: datetime,
        city_ids: Optional[list[int]] = None,
        service_types: Optional[list[int]] = None,
        hexagons: Optional[list[int]] = None,
    ) -> pd.DataFrame:
        """
        Load kandoo parameter data from ClickHouse.
        
        Args:
            from_date: Start date for query
            to_date: End date for query
            city_ids: Optional filter for city IDs
            service_types: Optional filter for service types
            hexagons: Optional filter for hexagon IDs
            
        Returns:
            DataFrame with kandoo parameter data
        """
        client = self._get_client()
        
        # Build WHERE clause
        conditions = [
            f"from >= '{from_date.isoformat()}'",
            f"to <= '{to_date.isoformat()}'",
        ]
        
        if city_ids:
            city_list = ",".join(str(cid) for cid in city_ids)
            conditions.append(f"city_id IN ({city_list})")
        
        if service_types:
            service_list = ",".join(str(st) for st in service_types)
            conditions.append(f"service_type IN ({service_list})")
        
        if hexagons:
            hex_list = ",".join(str(h) for h in hexagons)
            conditions.append(f"hex_id IN ({hex_list})")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        SELECT 
            from,
            to,
            city_id,
            hex_id,
            service_type,
            surge_percent,
            surge_absolute,
            cumulative_surge_percent,
            cumulative_surge_absolute,
            rule_id,
            increase_factor,
            increase_absolute,
            decrease_factor,
            decrease_absolute,
            resolution,
            reason,
            absolute_reason,
            expansion_level,
            price_cnvr,
            accept_rate,
            rule_sheet_id,
            rule_sheet_title,
            rule_sheet_ar,
            rule_sheet_pc,
            created_date,
            clickhouse_time
        FROM {self.cfg.database}.{self.cfg.table}
        WHERE {where_clause}
        ORDER BY from, hex_id, service_type
        """
        
        logger.info(f"Querying ClickHouse: {query[:200]}...")
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        
        if not df.empty:
            # Convert datetime columns
            df["from"] = pd.to_datetime(df["from"])
            df["to"] = pd.to_datetime(df["to"])
            df["created_date"] = pd.to_datetime(df["created_date"])
            df["clickhouse_time"] = pd.to_datetime(df["clickhouse_time"])
            
            logger.info(f"Loaded {len(df)} rows from ClickHouse")
        else:
            logger.warning("No data found in ClickHouse for the specified criteria")
        
        return df

    def create_import_tasks_from_dataframe(self, df: pd.DataFrame) -> Iterable[ImportTask]:
        """
        Convert ClickHouse DataFrame to ImportTask objects.
        
        Args:
            df: DataFrame from load_parameters()
            
        Yields:
            ImportTask objects
        """
        for _, row in df.iterrows():
            try:
                period = PeriodWindow(
                    start=row["from"],
                    end=row["to"],
                    duration=row["to"] - row["from"],
                    collect_duration=row["to"] - row["from"],
                )
                
                task = ImportTask(
                    hexagon=int(row["hex_id"]),
                    service_type=int(row["service_type"]),
                    city_id=int(row.get("city_id", 0)),
                    period=period,
                    expansion_weight=None,  # Not available in kandoo.parameter
                )
                yield task
            except (KeyError, ValueError, TypeError) as exc:
                logger.error(f"Failed to create ImportTask from row: {exc}")
                continue

    def load_and_create_tasks(
        self,
        from_date: datetime,
        to_date: datetime,
        city_ids: Optional[list[int]] = None,
        service_types: Optional[list[int]] = None,
        hexagons: Optional[list[int]] = None,
    ) -> Iterable[ImportTask]:
        """
        Load data from ClickHouse and create ImportTask objects.
        
        This is a convenience method that combines load_parameters and create_import_tasks_from_dataframe.
        """
        df = self.load_parameters(from_date, to_date, city_ids, service_types, hexagons)
        return self.create_import_tasks_from_dataframe(df)

