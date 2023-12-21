"""Parquet time series storage"""

from datetime import datetime

from infra_sys.time_series_models import TimeSeriesData, TimeSeriesMetadata
from infra_sys.time_series_storage_base import TimeSeriesStorageBase


class ParquetTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in Parquet files."""

    def add_time_series(self, time_series: TimeSeriesData) -> None:
        ...

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        ...

    def remove_time_series(self, metadata: TimeSeriesMetadata) -> TimeSeriesData:
        ...
