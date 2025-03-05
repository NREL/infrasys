"""In-memory time series storage"""

from datetime import datetime
from pathlib import Path
from typing import Any, TypeAlias

from numpy.typing import NDArray
from uuid import UUID
from loguru import logger

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    NonSequentialTimeSeries,
    NonSequentialTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

DataStoreType: TypeAlias = NDArray | tuple[NDArray, NDArray]


class InMemoryTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in memory."""

    def __init__(self) -> None:
        self._arrays: dict[UUID, DataStoreType] = {}  # Time series UUID, not metadata UUID
        self._ts_metadata_type: str | None = None

    def get_time_series_directory(self) -> None:
        return None

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        connection: Any = None,
    ) -> None:
        if isinstance(time_series, (SingleTimeSeries, NonSequentialTimeSeries)):
            if metadata.time_series_uuid not in self._arrays:
                self._arrays[metadata.time_series_uuid] = (
                    (
                        time_series.data_array,
                        time_series.timestamps,
                    )
                    if hasattr(time_series, "timestamps")
                    else time_series.data_array
                )
                self._ts_metadata_type = metadata.type
                logger.debug("Added {} to store", time_series.summary)
            else:
                logger.debug("{} was already stored", time_series.summary)
        else:
            msg = f"add_time_series not implemented for {type(time_series)}"
            raise NotImplementedError(msg)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: Any = None,
    ) -> TimeSeriesData:
        if isinstance(metadata, SingleTimeSeriesMetadata):
            return self._get_single_time_series(metadata, start_time, length)
        elif isinstance(metadata, NonSequentialTimeSeriesMetadata):
            return self._get_nonsequential_time_series(metadata)
        raise NotImplementedError(str(metadata.get_time_series_data_type()))

    def remove_time_series(self, metadata: TimeSeriesMetadata, connection: Any = None) -> None:
        time_series = self._arrays.pop(metadata.time_series_uuid, None)
        if time_series is None:
            msg = f"No time series with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)

    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Path | str | None = None
    ) -> None:
        msg = "Bug: InMemoryTimeSeriesStorage.serialize should never be called."
        raise Exception(msg)

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        ts_data: NDArray | None
        ts_data = self._arrays.get(metadata.time_series_uuid)  # type: ignore
        if ts_data is None:
            msg = f"No time series with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)

        if start_time or length:
            index, length = metadata.get_range(start_time=start_time, length=length)
            ts_data = ts_data[index : index + length]

        if metadata.quantity_metadata is not None:
            ts_data = metadata.quantity_metadata.quantity_type(
                ts_data, metadata.quantity_metadata.units
            )
        assert ts_data is not None
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=ts_data,
            normalization=metadata.normalization,
        )

    def _get_nonsequential_time_series(
        self,
        metadata: NonSequentialTimeSeriesMetadata,
    ) -> NonSequentialTimeSeries:
        ts_data, ts_timestamps = self._arrays.get(metadata.time_series_uuid, (None, None))
        if ts_data is None:
            msg = f"No time series data with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)

        if ts_timestamps is None:
            msg = f"No time series timestamps with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)

        if metadata.quantity_metadata is not None:
            ts_data = metadata.quantity_metadata.quantity_type(
                ts_data, metadata.quantity_metadata.units
            )
        assert ts_data is not None
        assert ts_timestamps is not None
        return NonSequentialTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            data=ts_data,
            timestamps=ts_timestamps,
            normalization=metadata.normalization,
        )
