"""In-memory time series storage"""

import logging
from datetime import datetime
from uuid import UUID

from infra_sys.exceptions import ISNotStored
from infra_sys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
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

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        time_series = self._arrays.get(metadata.uuid)
        if time_series is None:
            msg = f"No time series with {metadata.uuid=} is stored"
            raise ISNotStored(msg)

        if metadata.get_time_series_data_type() == SingleTimeSeries:
            return self._get_single_time_series(metadata, start_time=start_time, length=length)
        raise NotImplementedError(str(metadata.get_time_series_data_type()))

    def remove_time_series(self, metadata: TimeSeriesMetadata) -> TimeSeriesData:
        time_series = self._arrays.pop(metadata.uuid, None)
        if time_series is None:
            msg = f"No time series with {metadata.uuid=} is stored"
            raise ISNotStored(msg)
        return time_series

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        base_ts = self._arrays[metadata.uuid]
        if start_time is None and length is None:
            return base_ts

        index, length = metadata.get_range(start_time=start_time, length=length)
        return SingleTimeSeries(
            name=base_ts.name,
            resolution=base_ts.resolution,
            initial_time=start_time or base_ts.initial_time,
            length=length,
            data=base_ts.data[index : index + length],
        )
