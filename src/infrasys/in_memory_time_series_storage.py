"""In-memory time series storage"""

from datetime import datetime
from pathlib import Path
import numpy as np
from numpy.typing import NDArray
from typing import Optional, TypeAlias
from uuid import UUID
import pint

from loguru import logger
from infrasys.arrow_storage import ArrowTimeSeriesStorage

from infrasys.exceptions import ISNotStored, ISOperationNotAllowed
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

DataStoreType: TypeAlias = NDArray


class InMemoryTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in memory."""

    def __init__(self) -> None:
        self._arrays: dict[UUID, DataStoreType] = {}  # Time series UUID, not metadata UUID

    def get_time_series_directory(self) -> None:
        return None

    def add_time_series(self, metadata: TimeSeriesMetadata, time_series: TimeSeriesData) -> None:
        if metadata.time_series_uuid not in self._arrays:
            time_series_array = time_series.data
            if isinstance(time_series_array, pint.Quantity):
                time_series_array = time_series_array.magnitude
            self._arrays[metadata.time_series_uuid] = time_series_array
            logger.debug("Added {} to store", time_series.summary)
        else:
            logger.debug("{} was already stored", time_series.summary)

    def _add_raw_single_time_series(
        self, time_series_uuid: UUID, time_series_data: DataStoreType
    ) -> None:
        if time_series_uuid not in self._arrays:
            self._arrays[time_series_uuid] = time_series_data
            logger.debug("Added {} to store", time_series_uuid)
        else:
            logger.debug("{} was already stored", time_series_uuid)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        if isinstance(metadata, SingleTimeSeriesMetadata):
            return self._get_single_time_series(metadata, start_time, length)
        raise NotImplementedError(str(metadata.get_time_series_data_type()))

    def _get_raw_single_time_series(self, time_series_uuid: UUID) -> NDArray:
        data_array = self._arrays[time_series_uuid]
        if not isinstance(data_array, np.ndarray):
            msg = f"Can't retrieve type: {type(data_array)} as single_time_series"
            raise ISOperationNotAllowed(msg)
        return data_array

    def remove_time_series(self, uuid: UUID) -> None:
        time_series = self._arrays.pop(uuid, None)
        if time_series is None:
            msg = f"No time series with {uuid} is stored"
            raise ISNotStored(msg)

    def serialize(self, dst: Path | str, _: Optional[Path | str] = None) -> None:
        base_directory = dst if isinstance(dst, Path) else Path(dst)
        storage = ArrowTimeSeriesStorage.create_with_permanent_directory(base_directory)
        for ts_uuid, ts in self._arrays.items():
            storage._add_raw_single_time_series(ts_uuid, ts)

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        base_ts = self._arrays[metadata.time_series_uuid]
        assert isinstance(base_ts, np.ndarray)
        if start_time is None and length is None:
            ts_data = base_ts
        else:
            index, length = metadata.get_range(start_time=start_time, length=length)
            ts_data = base_ts[index : index + length]
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=ts_data,
            normalization=metadata.normalization,
        )
