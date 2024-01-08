"""Defines the base class for time series storage."""


import abc
from datetime import datetime

from infra_sys.time_series_models import TimeSeriesData, TimeSeriesMetadata


class TimeSeriesStorageBase(abc.ABC):
    """Base class for time series storage"""

    @abc.abstractmethod
    def add_time_series(self, time_series: TimeSeriesData) -> None:
        """Store a time series array."""

    @abc.abstractmethod
    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def remove_time_series(self, metadata: TimeSeriesMetadata) -> TimeSeriesData:
        """Remove a time series array and return it."""
