"""In-memory time series storage"""

from datetime import datetime
from uuid import UUID

from loguru import logger

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase


class InMemoryTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in memory."""

    def __init__(self):
        self._arrays: dict[UUID, TimeSeriesData] = {}  # This is metadata UUID, not time series
        # TODO: consider storing by time series by UUID instead. Would have to track reference
        # counts.

    def add_time_series(self, metadata: TimeSeriesMetadata, time_series: TimeSeriesData) -> None:
        if metadata.uuid not in self._arrays:
            self._arrays[metadata.uuid] = time_series
            logger.debug("Added {} to store", time_series.summary)
        else:
            logger.debug("{} was already stored", time_series.summary)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        time_series = self._arrays.get(metadata.uuid)
        if time_series is None:
            msg = f"No time series with {metadata.uuid} is stored"
            raise ISNotStored(msg)

        if metadata.get_time_series_data_type() == SingleTimeSeries:
            return self._get_single_time_series(metadata, start_time=start_time, length=length)
        raise NotImplementedError(str(metadata.get_time_series_data_type()))

    def remove_time_series(self, metadata: TimeSeriesMetadata) -> None:
        time_series = self._arrays.pop(metadata.uuid, None)
        if time_series is None:
            msg = f"No time series with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)

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
            variable_name=base_ts.variable_name,
            resolution=base_ts.resolution,
            initial_time=start_time or base_ts.initial_time,
            length=length,
            data=base_ts.data[index : index + length],
        )
