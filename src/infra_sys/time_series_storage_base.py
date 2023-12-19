"""Defines the base class for time series storage."""


import abc
from typing import Type
from uuid import UUID

from infra_sys.time_series_models import SingleTimeSeries, TimeSeriesData


class TimeSeriesStorageBase(abc.ABC):
    """Base class for time series storage"""

    @abc.abstractmethod
    def add_time_series(self, time_series: TimeSeriesData) -> None:
        """Store a time series array."""

    @abc.abstractmethod
    def get_time_series(
        self, name: str, time_series_type: Type = SingleTimeSeries
    ) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def get_time_series_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def remove_time_series(self, name: str, time_series_type=SingleTimeSeries) -> TimeSeriesData:
        """Remove a time series array and return it."""

    @abc.abstractmethod
    def remove_time_series_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        """Remove a time series array and return it."""
