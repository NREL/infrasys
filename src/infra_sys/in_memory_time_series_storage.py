"""In-memory time series storage"""

import logging
from uuid import UUID

from infra_sys.exceptions import ISNotStored
from infra_sys.time_series_models import (
    TimeSeriesData,
)
from infra_sys.time_series_storage_base import TimeSeriesStorageBase

logger = logging.getLogger(__name__)


class InMemoryTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in memory."""

    def __init__(self):
        self._arrays: dict[UUID, TimeSeriesData] = {}

    def add_time_series(self, time_series: TimeSeriesData) -> None:
        if time_series.uuid not in self._arrays:
            self._arrays[time_series.uuid] = time_series
            logger.debug("Added %s to store", time_series.summary)
        else:
            logger.debug("%s was already stored", time_series.summary)

    def get_time_series(self, uuid: UUID) -> TimeSeriesData:
        time_series = self._arrays.get(uuid)
        if time_series is None:
            msg = f"No time series with {uuid=} is stored"
            raise ISNotStored(msg)
        return time_series

    def remove_time_series(self, uuid: UUID) -> TimeSeriesData:
        time_series = self._arrays.pop(uuid, None)
        if time_series is None:
            msg = f"No time series with {uuid=} is stored"
            raise ISNotStored(msg)
        return time_series
