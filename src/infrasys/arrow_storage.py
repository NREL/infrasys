"""Implementation of arrow storage for time series."""

import atexit
import shutil
from datetime import datetime
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Optional
from uuid import UUID

import pyarrow as pa
from loguru import logger

from infrasys.exceptions import ISNotStored
from infrasys.base_quantity import BaseQuantity
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

EXTENSION = ".arrow"


class ArrowTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in disk"""

    def __init__(self, directory: Path) -> None:
        self._ts_directory = directory

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

    def get_time_series_directory(self) -> Path:
        return self._ts_directory

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
    ) -> None:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        if not fpath.exists():
            if isinstance(time_series, SingleTimeSeries):
                arrow_batch = self._convert_to_record_batch(time_series, metadata.variable_name)
                with pa.OSFile(str(fpath), "wb") as sink:  # type: ignore
                    with pa.ipc.new_file(sink, arrow_batch.schema) as writer:
                        writer.write(arrow_batch)
            else:
                msg = f"Bug: need to implement add_time_series for {type(time_series)}"
                raise NotImplementedError(msg)
            logger.trace("Saving time series to {}", fpath)
            logger.debug("Added {} to time series storage", time_series.summary)
        else:
            logger.debug("{} was already stored", time_series.summary)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> Any:
        if isinstance(metadata, SingleTimeSeriesMetadata):
            return self._get_single_time_series(
                metadata=metadata, start_time=start_time, length=length
            )

        msg = f"Bug: need to implement get_time_series for {type(metadata)}"
        raise NotImplementedError(msg)

    def remove_time_series(self, uuid: UUID) -> None:
        fpath = self._ts_directory.joinpath(f"{uuid}{EXTENSION}")
        if not fpath.exists():
            msg = f"No time series with {uuid} is stored"
            raise ISNotStored(msg)
        fpath.unlink()

    def serialize(self, dst: Path | str, src: Optional[Path | str] = None) -> None:
        # From the shutil documentation: the copying operation will continue if
        # it encounters existing directories, and files within the dst tree
        # will be overwritten by corresponding files from the src tree.
        if src is None:
            src = self._ts_directory
        shutil.copytree(src, dst, dirs_exist_ok=True)
        logger.info("Copied time series data to {}", dst)

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        fpath = self._ts_directory.joinpath(f"{metadata.time_series_uuid}{EXTENSION}")
        with pa.memory_map(str(fpath), "r") as source:
            base_ts = pa.ipc.open_file(source).get_record_batch(0)
            logger.trace("Reading time series from {}", fpath)
        index, length = metadata.get_range(start_time=start_time, length=length)
        data = base_ts[metadata.variable_name][index : index + length]
        if metadata.quantity_metadata is not None:
            data = metadata.quantity_metadata.quantity_type(data, metadata.quantity_metadata.units)
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=data,
            normalization=metadata.normalization,
        )

    def _convert_to_record_batch(self, array: SingleTimeSeries, variable_name: str):
        """Create record batch to save array to disk."""
        pa_array = array.data.magnitude if isinstance(array.data, BaseQuantity) else array.data
        assert isinstance(pa_array, pa.Array)
        schema = pa.schema([pa.field(variable_name, pa_array.type)])
        return pa.record_batch([pa_array], schema=schema)


def clean_tmp_folder(folder: Path | str) -> None:
    shutil.rmtree(folder)
    logger.info("Wiped time series folder: {}", folder)
