"""Defines the base class for time series storage."""

import abc
from datetime import datetime
from pathlib import Path
from typing import Optional, Any
from uuid import UUID

from infrasys.time_series_models import TimeSeriesData, TimeSeriesMetadata


class TimeSeriesStorageBase(abc.ABC):
    """Base class for time series storage"""

    @abc.abstractmethod
    def add_time_series(self, metadata: TimeSeriesMetadata, time_series: TimeSeriesData) -> None:
        """Store a time series array."""

    @abc.abstractmethod
    def add_raw_single_time_series(self, time_series_uuid: UUID, time_series_data: Any) -> None:
        """Store a time series array from raw data."""

    @abc.abstractmethod
    def get_raw_single_time_series(self, time_series_uuid: UUID) -> Any:
        """Get the raw time series data for a given uuid."""

    @abc.abstractmethod
    def get_time_series_directory(self) -> Path | None:
        """Return the directory containing time series files. Will be none for in-memory time
        series.
        """

    @abc.abstractmethod
    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def remove_time_series(self, uuid: UUID) -> None:
        """Remove a time series array and return it."""

    @abc.abstractmethod
    def serialize(self, dst: Path | str, src: Optional[Path | str] = None) -> None:
        """Serialize all time series to the destination directory."""
