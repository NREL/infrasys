"""Implementation of arrow storage for time series."""

import atexit
import shutil
from datetime import datetime
from functools import singledispatchmethod
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Optional

import numpy as np
import pyarrow as pa
from loguru import logger
from numpy.typing import NDArray

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import (
    AbstractDeterministic,
    Deterministic,
    DeterministicMetadata,
    DeterministicTimeSeriesType,
    NonSequentialTimeSeries,
    NonSequentialTimeSeriesMetadata,
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
    TimeSeriesStorageType,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

EXTENSION = ".arrow"


class ArrowTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in disk"""

    def __init__(self, directory: Path) -> None:
        self._ts_directory = directory
        self._ts_metadata: str | None = None

    @classmethod
    def create_with_temp_directory(
        cls, base_directory: Optional[Path] = None
    ) -> "ArrowTimeSeriesStorage":
        """Construct ArrowTimeSeriesStorage with a temporary directory."""
        directory = Path(mkdtemp(dir=base_directory))
        logger.debug("Creating tmp folder at {}", directory)
        atexit.register(clean_tmp_folder, directory)
        return cls(directory)

    @classmethod
    def create_with_permanent_directory(cls, directory: Path) -> "ArrowTimeSeriesStorage":
        """Construct ArrowTimeSeriesStorage with a permanent directory."""
        directory.mkdir(exist_ok=True)
        return cls(directory)

    @classmethod
    def deserialize(
        cls,
        data: dict[str, Any],
        time_series_dir: Path,
        dst_time_series_directory: Path | None,
        read_only: bool,
        **kwargs: Any,
    ) -> tuple["ArrowTimeSeriesStorage", None]:
        """Deserialize Arrow storage from serialized data."""
        if read_only:
            storage = cls.create_with_permanent_directory(time_series_dir)
        else:
            storage = cls.create_with_temp_directory(base_directory=dst_time_series_directory)
            storage.serialize({}, storage.get_time_series_directory(), src=time_series_dir)
        return storage, None

    def get_time_series_directory(self) -> Path:
        return self._ts_directory

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        context: Any = None,
    ) -> None:
        self._add_time_series(time_series)

    @singledispatchmethod
    def _add_time_series(self, time_series) -> None:
        msg = f"Bug: need to implement add_time_series for {type(time_series)}"
        raise NotImplementedError(msg)

    @_add_time_series.register(SingleTimeSeries)
    def _(self, time_series):
        time_series_data = time_series.data_array
        time_series_uuid = time_series.uuid
        fpath = self._ts_directory.joinpath(f"{time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            arrow_batch = self._convert_to_record_batch_single_time_series(
                time_series_data, str(time_series_uuid)
            )
            with pa.OSFile(str(fpath), "wb") as sink:  # type: ignore
                with pa.ipc.new_file(sink, arrow_batch.schema) as writer:
                    writer.write(arrow_batch)
            logger.trace("Saving time series to {}", fpath)
            logger.debug("Added {} to time series storage", time_series_uuid)
        else:
            logger.debug("{} was already stored", time_series_uuid)

    @_add_time_series.register(NonSequentialTimeSeries)
    def _(self, time_series):
        time_series_data = (time_series.data_array, time_series.timestamps_array)
        time_series_uuid = time_series.uuid
        fpath = self._ts_directory.joinpath(f"{time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            arrow_batch = self._convert_to_record_batch_nonsequential_time_series(time_series_data)
            with pa.OSFile(str(fpath), "wb") as sink:  # type: ignore
                with pa.ipc.new_file(sink, arrow_batch.schema) as writer:
                    writer.write(arrow_batch)
            logger.trace("Saving time series to {}", fpath)
            logger.debug("Added {} to time series storage", time_series_uuid)
        else:
            logger.debug("{} was already stored", time_series_uuid)

    @_add_time_series.register(AbstractDeterministic)
    def _(self, time_series):
        """Store deterministic forecast time series data as a 2D matrix.

        Each row represents a forecast window, and each column represents a time step
        in the forecast horizon. The data is stored as a single array of arrays.
        """
        time_series_uuid = time_series.uuid
        fpath = self._ts_directory.joinpath(f"{time_series_uuid}{EXTENSION}")

        if not fpath.exists():
            forecast_data = time_series.data_array

            forecast_list = forecast_data.tolist()

            schema = pa.schema([pa.field(str(time_series_uuid), pa.list_(pa.list_(pa.float64())))])

            arrow_batch = pa.record_batch([pa.array([forecast_list])], schema=schema)

            # Write to disk
            with pa.OSFile(str(fpath), "wb") as sink:  # type: ignore
                with pa.ipc.new_file(sink, arrow_batch.schema) as writer:
                    writer.write(arrow_batch)

            logger.trace("Saving deterministic time series to {}", fpath)
            logger.debug("Added {} to time series storage", time_series_uuid)
        else:
            logger.debug("{} was already stored", time_series_uuid)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> TimeSeriesData:
        """Return a time series array using the appropriate handler based on metadata type."""
        return self._get_time_series_dispatch(metadata, start_time, length, context)

    @singledispatchmethod
    def _get_time_series_dispatch(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> TimeSeriesData:
        msg = f"Bug: need to implement get_time_series for {type(metadata)}"
        raise NotImplementedError(msg)

    @_get_time_series_dispatch.register(SingleTimeSeriesMetadata)
    def _(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> SingleTimeSeries:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        with pa.memory_map(str(fpath), "r") as source:
            base_ts = pa.ipc.open_file(source).get_record_batch(0)
            logger.trace("Reading time series from {}", fpath)
        index, length = metadata.get_range(start_time=start_time, length=length)
        columns = base_ts.column_names
        if len(columns) != 1:
            msg = f"Bug: expected a single column: {columns=}"
            raise Exception(msg)
        column = columns[0]
        data = base_ts[column][index : index + length]
        if metadata.units is not None:
            np_array = metadata.units.quantity_type(data, metadata.units.units)
        else:
            np_array = np.array(data)
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            name=metadata.name,
            resolution=metadata.resolution,
            initial_timestamp=start_time or metadata.initial_timestamp,
            data=np_array,
            normalization=metadata.normalization,
        )

    @_get_time_series_dispatch.register(NonSequentialTimeSeriesMetadata)
    def _(
        self,
        metadata: NonSequentialTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> NonSequentialTimeSeries:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        with pa.memory_map(str(fpath), "r") as source:
            base_ts = pa.ipc.open_file(source).get_record_batch(0)
            logger.trace("Reading time series from {}", fpath)
        columns = base_ts.column_names
        if len(columns) != 2:
            msg = f"Bug: expected two columns: {columns=}"
            raise Exception(msg)
        data_column, timestamps_column = columns[0], columns[1]
        data, timestamps = (
            base_ts[data_column],
            base_ts[timestamps_column],
        )
        if metadata.units is not None:
            np_data_array = metadata.units.quantity_type(data, metadata.units.units)
        else:
            np_data_array = np.array(data)
        np_time_array = np.array(timestamps).astype("O")  # convert to datetime object
        return NonSequentialTimeSeries(
            uuid=metadata.time_series_uuid,
            name=metadata.name,
            data=np_data_array,
            timestamps=np_time_array,
            normalization=metadata.normalization,
        )

    @_get_time_series_dispatch.register(DeterministicMetadata)
    def _(
        self,
        metadata: DeterministicMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        context: Any = None,
    ) -> DeterministicTimeSeriesType:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        with pa.memory_map(str(fpath), "r") as source:
            base_ts = pa.ipc.open_file(source).get_record_batch(0)
            logger.trace("Reading time series from {}", fpath)

        columns = base_ts.column_names
        if len(columns) != 1:
            msg = f"Bug: expected a single column: {columns=}"
            raise Exception(msg)

        column = columns[0]
        data = base_ts[column][0]  # Get the nested array

        if metadata.units is not None:
            np_array = metadata.units.quantity_type(data, metadata.units.units)
        else:
            np_array = np.array(data)

        return Deterministic(
            uuid=metadata.time_series_uuid,
            name=metadata.name,
            resolution=metadata.resolution,
            initial_timestamp=metadata.initial_timestamp,
            horizon=metadata.horizon,
            interval=metadata.interval,
            window_count=metadata.window_count,
            data=np_array,
            normalization=metadata.normalization,
        )

    def remove_time_series(self, metadata: TimeSeriesMetadata, context: Any = None) -> None:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            msg = f"No time series with {metadata.time_series_uuid} is stored"
            raise ISNotStored(msg)
        fpath.unlink()

    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Path | str | None = None
    ) -> None:
        # From the shutil documentation: the copying operation will continue if
        # it encounters existing directories, and files within the dst tree
        # will be overwritten by corresponding files from the src tree.
        if src is None:
            src = self._ts_directory
        shutil.copytree(src, dst, dirs_exist_ok=True)
        self.add_serialized_data(data)
        logger.info("Copied time series data to {}", dst)

    @staticmethod
    def add_serialized_data(data: dict[str, Any]) -> None:
        data["time_series_storage_type"] = TimeSeriesStorageType.ARROW.value

    def _convert_to_record_batch_single_time_series(
        self, time_series_array: NDArray, column: str
    ) -> pa.RecordBatch:
        """Create record batch for SingleTimeSeries to save array to disk."""
        pa_array = pa.array(time_series_array)
        schema = pa.schema([pa.field(column, pa_array.type)])
        return pa.record_batch([pa_array], schema=schema)

    def _convert_to_record_batch_nonsequential_time_series(
        self, time_series_array: tuple[NDArray, NDArray]
    ) -> pa.RecordBatch:
        """Create record batch for NonSequentialTimeSeries to save array to disk."""
        data_array, timestamps_array = time_series_array
        pa_data_array = pa.array(data_array)
        pa_timestamps_array = pa.array(timestamps_array)

        schema = pa.schema(
            [
                pa.field("data", pa_data_array.type),
                pa.field("timestamp", pa_timestamps_array.type),
            ]
        )
        return pa.record_batch([pa_data_array, pa_timestamps_array], schema=schema)


def clean_tmp_folder(folder: Path | str) -> None:
    shutil.rmtree(folder)
    logger.info("Wiped time series folder: {}", folder)
