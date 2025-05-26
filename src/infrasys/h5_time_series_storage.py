import functools
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional

import h5py

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
    TimeSeriesStorageType,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

TIME_SERIES_DATA_FORMAT_VERSION = "1.0.0"
TIME_SERIES_VERSION_KEY = "data_format_version"


def file_handle(func):
    """Decorator to ensure a valid HDF5 file handle (context) is available.

    If 'context' is passed in kwargs and is not None, it's used directly.
    Otherwise, opens a new context using self.open_time_series_store()
    and passes the handle as the 'context' kwarg to the wrapped function.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        context = kwargs.get("context")
        if context is not None:
            return func(self, *args, **kwargs)
        else:
            with self.open_time_series_store("a") as file_handle:
                kwargs["context"] = file_handle
                return func(self, *args, **kwargs)

    return wrapper


class HDF5TimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in an h5 file."""

    STORAGE_FILE = "time_series_storage.h5"
    HDF5_TS_ROOT_PATH = "time_series"
    HDF5_TS_METADATA_ROOT_PATH = "time_series_metadata"

    def __init__(
        self,
        directory: Path,
        filename: str | None = None,
        **kwargs,
    ) -> None:
        """Initialize the HDF5 time series storage.

        Parameters
        ----------
        directory : Path
            Directory to store the HDF5 file
        """
        self.directory = directory
        self._fpath = self.directory / self.STORAGE_FILE
        self._file_handle = None

        self._check_root()

    @contextmanager
    def open_time_series_store(self, mode: str = "a") -> Generator[h5py.File, None, None]:
        assert self._fpath
        self._file_handle = None

        # H5PY ensures closing of the file after the with statement.
        with h5py.File(self._fpath, mode=mode) as file_handle:
            yield file_handle

    def get_time_series_directory(self) -> Path:
        return self.directory

    def _check_root(self) -> None:
        """Check the root group exist on the hdf5."""
        with self.open_time_series_store() as file_handle:
            if self.HDF5_TS_ROOT_PATH not in file_handle:
                root = file_handle.create_group(self.HDF5_TS_ROOT_PATH)
                root.attrs[TIME_SERIES_VERSION_KEY] = TIME_SERIES_DATA_FORMAT_VERSION

            if self.HDF5_TS_METADATA_ROOT_PATH not in file_handle:
                file_handle.create_group(self.HDF5_TS_METADATA_ROOT_PATH)
        return

    def _serialize_compression_settings(self) -> None:
        """Add default compression settings."""
        with self.open_time_series_store() as file_handle:
            root = file_handle[self.HDF5_TS_ROOT_PATH]
            root.attrs["compression_enabled"] = False
            root.attrs["compression_type"] = "DEFLATE"
            root.attrs["compression_level"] = 3
            root.attrs["compression_shuffle"] = True
        return None

    @staticmethod
    def add_serialized_data(data: dict[str, Any]) -> None:
        data["time_series_storage_type"] = str(TimeSeriesStorageType.HDF5)

    @file_handle
    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        context: Any = None,
    ) -> None:
        """Store a time series array.

        Parameters
        ----------
        metadata : TimeSeriesMetadata
            Metadata for the time series
        time_series : TimeSeriesData
            Time series data to store
        context : Any, optional
            Optional context parameter
        """
        assert isinstance(time_series, SingleTimeSeries)
        assert isinstance(metadata, SingleTimeSeriesMetadata)
        root = context[self.HDF5_TS_ROOT_PATH]
        uuid = str(metadata.time_series_uuid)

        if uuid not in root:
            group = root.create_group(uuid)

            group.create_dataset(
                "data", data=time_series.data, compression="gzip", compression_opts=5
            )

            group.attrs["type"] = metadata.type
            group.attrs["initial_timestamp"] = metadata.initial_timestamp.isoformat()
            group.attrs["resolution"] = metadata.resolution.total_seconds()
            group.attrs["module"] = "InfrastructureSystems"
            group.attrs["data_type"] = "Float64"

    def get_metadata_store(self) -> sqlite3.Connection:
        """Get the metadata store.

        Returns
        -------
        TimeSeriesMetadataStore
            The metadata store
        """
        with self.open_time_series_store() as file_handle:
            ts_metadata = bytes(file_handle[self.HDF5_TS_METADATA_ROOT_PATH][:])
            conn = sqlite3.connect(":memory:")
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                temp_file_path = tmp.name
                tmp.write(ts_metadata)
                backup_conn = sqlite3.connect(temp_file_path)
                with conn:
                    backup_conn.backup(conn)
                backup_conn.close()
        return conn

    @file_handle
    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: Optional[datetime] = None,
        length: Optional[int] = None,
        context: Any = None,
    ) -> TimeSeriesData:
        """Return a time series array.

        Parameters
        ----------
        metadata : TimeSeriesMetadata
            Metadata for the time series to retrieve
        start_time : datetime, optional
            Optional start time to retrieve from
        length : int, optional
            Optional number of values to retrieve
        context: Any, optional
            Optional context for the data.

        Returns
        -------
        TimeSeriesData
            Retrieved time series data

        Raises
        ------
        ISNotStored
            If the time series with the specified UUID doesn't exist
        """
        assert context is not None
        assert isinstance(metadata, SingleTimeSeriesMetadata)

        root = context[self.HDF5_TS_ROOT_PATH]
        uuid = str(metadata.time_series_uuid)

        if uuid not in root:
            msg = f"Time series with {uuid=} not found"
            raise ISNotStored(msg)

        dataset = root[uuid]["data"]

        index, length = metadata.get_range(start_time=start_time, length=length)
        data = dataset[index : index + length]
        if metadata.units is not None:
            data = metadata.units.quantity_type(data, metadata.units.units)
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            name=metadata.name,
            resolution=metadata.resolution,
            initial_timestamp=start_time or metadata.initial_timestamp,
            data=data,
            normalization=metadata.normalization,
        )

    @file_handle
    def remove_time_series(self, metadata: TimeSeriesMetadata, context: Any = None) -> None:
        """Remove a time series array.

        Parameters
        ----------
        metadata : TimeSeriesMetadata
            Metadata for the time series to remove
        context : Any, optional
            Optional context data

        Raises
        ------
        ISNotStored
            If the time series with the specified UUID doesn't exist
        """
        uuid = str(metadata.time_series_uuid)

        if uuid not in context[self.HDF5_TS_ROOT_PATH]:
            msg = f"Time series with {uuid=} not found"
            raise ISNotStored(msg)
        del context[self.HDF5_TS_ROOT_PATH][uuid]

        # meta_group = context[self.HDF5_TS_METADATA_ROOT_PATH]
        # if uuid in meta_group:
        #     del meta_group[uuid]

    @file_handle
    def batch_remove_time_series(
        self, metadata_list: list[TimeSeriesMetadata], context: Any = None
    ) -> None:
        """Remove multiple time series arrays in a single operation.

        Parameters
        ----------
        metadata_list : list[TimeSeriesMetadata]
            List of metadata for the time series to remove
        context : Any, optional
            Optional context data
        """
        root = context[self.HDF5_TS_ROOT_PATH]
        meta_group = context[self.HDF5_TS_METADATA_ROOT_PATH]

        for metadata in metadata_list:
            uuid = str(metadata.time_series_uuid)
            if uuid in root:
                del root[uuid]

            if uuid in meta_group:
                del meta_group[uuid]

    def massive_batch_remove(self, metadata_list: list[TimeSeriesMetadata]) -> None:
        """Ultra-fast removal for very large numbers of time series (10K+)."""
        import os
        import shutil

        # Create a set of UUIDs to remove for fast lookups
        uuids_to_remove = {str(metadata.time_series_uuid) for metadata in metadata_list}

        # Create temporary file path
        temp_file = self._fpath.with_suffix(".temp.h5")

        # Copy only what we want to keep to the new file
        with h5py.File(self._fpath, "r") as src_file, h5py.File(temp_file, "w") as dst_file:
            # Copy all groups except time series data
            for key in src_file.keys():
                if key != self.HDF5_TS_ROOT_PATH and key != self.HDF5_TS_METADATA_ROOT_PATH:
                    h5py.h5o.copy(
                        src_file.id, key.encode("utf-8"), dst_file.id, key.encode("utf-8")
                    )

            # Create the main groups
            ts_root = dst_file.create_group(self.HDF5_TS_ROOT_PATH)
            _ = dst_file.create_group(self.HDF5_TS_METADATA_ROOT_PATH)

            # Copy attributes
            for attr_name, attr_value in src_file[self.HDF5_TS_ROOT_PATH].attrs.items():
                ts_root.attrs[attr_name] = attr_value

            # Copy only time series we want to keep
            src_root = src_file[self.HDF5_TS_ROOT_PATH]
            for uuid in src_root:
                if uuid not in uuids_to_remove:
                    h5py.h5o.copy(
                        src_file.id,
                        f"{self.HDF5_TS_ROOT_PATH}/{uuid}".encode("utf-8"),
                        dst_file.id,
                        f"{self.HDF5_TS_ROOT_PATH}/{uuid}".encode("utf-8"),
                    )

        # Replace original file with new one
        os.remove(self._fpath)
        shutil.move(temp_file, self._fpath)

    def serialize(
        self,
        data: dict[str, Any],
        dst: Path | str,
        src: Optional[Path | str] = None,
    ) -> None:
        """Serialize all time series to the destination directory.

        Parameters
        ----------
        data : Dict[str, Any]
            Additional data to serialize (not used in this implementation)
        dst : Path or str
            Destination directory or file path
        src : Path or str, optional
            Optional source directory or file path
        """
        dst_path = Path(dst) / self.STORAGE_FILE if Path(dst).is_dir() else Path(dst)
        self.output_file = dst_path
        self._serialize_compression_settings()
        with self.open_time_series_store() as f:
            f.flush()
            with h5py.File(dst_path, "a") as dst_file:
                if self.HDF5_TS_ROOT_PATH in f:
                    h5py.h5o.copy(
                        f.id,
                        self.HDF5_TS_ROOT_PATH.encode("utf-8"),
                        dst_file.id,
                        self.HDF5_TS_ROOT_PATH.encode("utf-8"),
                    )
                    if self.HDF5_TS_METADATA_ROOT_PATH in dst_file:
                        del dst_file[self.HDF5_TS_METADATA_ROOT_PATH]

        data["time_series_storage_file"] = str(dst_path)
        self.add_serialized_data(data)
