"""Defines the base class for time series storage."""

import abc
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional

from infrasys.time_series_models import TimeSeriesData, TimeSeriesMetadata


class TimeSeriesStorageBase(abc.ABC):
    """Base class for time series storage"""

    @abc.abstractmethod
    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        context: Any = None,
    ) -> None:
        """Store a time series array."""

    @abc.abstractmethod
    def get_time_series_directory(self) -> Path | None:
        """Return the directory containing time series files. Will be None for in-memory time
        series.
        """

    @abc.abstractmethod
    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> TimeSeriesData:
        """Return a time series array."""

    @abc.abstractmethod
    def remove_time_series(self, metadata: TimeSeriesMetadata, context: Any = None) -> None:
        """Remove a time series array."""

    @abc.abstractmethod
    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Optional[Path | str] = None
    ) -> None:
        """Serialize all time series to the destination directory."""

    @contextmanager
    def open_time_series_store(self, mode: str) -> Generator[Any, None, None]:
        """Open a connection to the time series store."""
        yield None
