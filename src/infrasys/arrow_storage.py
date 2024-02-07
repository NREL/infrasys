"""Implementation of arrow storage for time series."""

from datetime import datetime

import pyarrow as pa
from loguru import logger

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import SingleTimeSeries, SingleTimeSeriesMetadata
from infrasys.time_series_models import TimeSeriesData, TimeSeriesMetadata
from infrasys.time_series_storage_base import TimeSeriesStorageBase

EXTENSION = ".arrow"


class ArrowTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in disk"""

    def __init__(self, ts_directory) -> None:
        self._ts_directory = ts_directory

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
    ) -> None:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            arrow_batch = self._convert_to_record_batch(time_series, metadata.variable_name)
            with pa.OSFile(str(fpath), "wb") as sink:
                with pa.ipc.new_file(sink, arrow_batch.schema) as writer:
                    writer.write(arrow_batch)
            logger.trace("Saving time series to {}", fpath)
            logger.debug("Added {} to time series storage", time_series.summary)
        else:
            logger.debug("{} was already stored", time_series.summary)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        return self._get_single_time_series(
            metadata=metadata, start_time=start_time, length=length
        )

    def remove_time_series(self, metadata: TimeSeriesMetadata) -> None:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            msg = f"No time series with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)
        fpath.unlink()

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        with pa.memory_map(str(fpath), "rb") as source:
            base_ts = pa.ipc.open_file(source).get_record_batch(0)
            logger.trace("Reading time series from {}", fpath)
        index, length = metadata.get_range(start_time=start_time, length=length)
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            length=length,
            data=base_ts[metadata.variable_name][index : index + length],
        )

    def _convert_to_record_batch(self, array: pa.array, variable_name: str):
        """Create record batch to save array to disk."""
        array = array.data  # Extracting it from SingleTimeSeries
        schema = pa.schema([pa.field(variable_name, array.type)])
        return pa.record_batch([array], schema=schema)
