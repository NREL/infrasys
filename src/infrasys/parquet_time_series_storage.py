"""Parquet time series storage"""

from datetime import datetime
from typing import Any
from uuid import UUID

from infrasys.time_series_models import TimeSeriesData, TimeSeriesMetadata
from infrasys.time_series_storage_base import TimeSeriesStorageBase


class ParquetTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in Parquet files."""

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        connection: Any = None,
    ) -> None:
        ...

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: Any = None,
    ) -> TimeSeriesData:
        msg = "ParquetTimeSeriesStorage.get_time_series"
        raise NotImplementedError(msg)

    def remove_time_series(self, time_series_uuid: UUID, connection: Any = None) -> None:
        ...
