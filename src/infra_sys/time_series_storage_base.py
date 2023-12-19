"""Defines the base class for time series storage."""


import abc
from uuid import UUID

from infra_sys.time_series_models import TimeSeriesData


class TimeSeriesStorageBase(abc.ABC):
    """Base class for time series storage"""

    @abc.abstractmethod
    def add_time_series(self, time_series: TimeSeriesData) -> None:
        """Store a time series array."""

    @abc.abstractmethod
    def get_time_series(self, uuid: UUID) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def has_time_series(self, uuid: UUID) -> bool:
        """Return true if the uuid is stored."""

    @abc.abstractmethod
    def remove_time_series(self, uuid: UUID) -> TimeSeriesData:
        """Remove a time series array and return it."""
