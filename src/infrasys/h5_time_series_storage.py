import contextlib
import functools
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import h5py
from loguru import logger

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


# NOTE: Not sure if we want to go this approach.
def h5_handle(func):
    """Decorator to wrap methods needing an HDF5 file handle.

    Handles acquiring the handle via self._get_file_handle() and passes
    it as the second argument ('f') to the wrapped function.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self._get_file_handle() as f:
            return func(self, f, *args, **kwargs)

    return wrapper


class HDF5TimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in an h5 file."""

    STORAGE_FILE = "time_series_storage.h5"
    HDF5_TS_ROOT_PATH = "time_series"
    HDF5_TS_METADATA_ROOT_PATH = "time_series_metadata"

    def __init__(
        self,
        directory: Path | None = None,
        time_series_storage_file: Path | str | None = None,
        mode: str = "a",
        in_memory: bool = False,
        **kwargs,
    ) -> None:
        """Initialize the HDF5 time series storage.

        Parameters
        ----------
        directory : Path
            Directory to store the HDF5 file
        permanent : bool, optional
            If True, the file will not be deleted when the object is destroyed
        in_memory : bool, optional
            If True, the HDF5 file will be kept in memory only
        """
        # self._ts_directory = directory
        self._in_memory = in_memory
        self.directory = directory
        self.fname = time_series_storage_file
        if self.directory and not self.fname:
            self.fname = self.directory / self.STORAGE_FILE
        # self._file_path = time_series_storage_file
        self.mode = mode

        # Variables to handle context manager
        self._file_handle = None
        self._in_context = False

        self._check_root()

    @h5_handle
    def _check_root(self, f_handle):
        """Get the root group for time series data.

        Returns
        -------
        h5py.Group
            Root group for time series data
        """
        if self.HDF5_TS_ROOT_PATH not in f_handle:
            root = f_handle.create_group(self.HDF5_TS_ROOT_PATH)
            root.attrs[TIME_SERIES_VERSION_KEY] = TIME_SERIES_DATA_FORMAT_VERSION

        if self.HDF5_TS_METADATA_ROOT_PATH not in f_handle:
            f_handle.create_group(self.HDF5_TS_METADATA_ROOT_PATH)
        return True

    @h5_handle
    def _serialize_compression_settings(self, f):
        """Add default compression settings."""
        root = f[self.HDF5_TS_ROOT_PATH]
        root.attrs["compression_enabled"] = False
        root.attrs["compression_type"] = "CompressionTypes.DEFLATE"
        root.attrs["compression_level"] = 3
        root.attrs["compression_shuffle"] = True
        return

    @staticmethod
    def add_serialized_data(data: dict[str, Any]) -> None:
        data["time_series_storage_type"] = str(TimeSeriesStorageType.HDF5)

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        connection: Any = None,
    ) -> None:
        """Store a time series array.

        Parameters
        ----------
        metadata : TimeSeriesMetadata
            Metadata for the time series
        time_series : TimeSeriesData
            Time series data to store
        connection : Any, optional
            Optional connection parameter (not used in this implementation)
        """
        assert isinstance(time_series, SingleTimeSeries)
        assert isinstance(metadata, SingleTimeSeriesMetadata)

        with self._get_file_handle() as f:
            root = f[self.HDF5_TS_ROOT_PATH]
            uuid = str(metadata.time_series_uuid)

            if uuid not in root:
                group = root.create_group(uuid)

                group.create_dataset(
                    "data", data=time_series.data, compression="gzip", compression_opts=5
                )

                group.attrs["type"] = metadata.type
                group.attrs["initial_timestamp"] = metadata.initial_time.isoformat()
                group.attrs["resolution"] = metadata.resolution.total_seconds()

    @h5_handle
    def get_metadata_store(self, f) -> sqlite3.Connection:
        """Get the metadata store.

        Returns
        -------
        TimeSeriesMetadataStore
            The metadata store
        """
        ts_metadata = bytes(f[self.HDF5_TS_METADATA_ROOT_PATH][:])
        conn = sqlite3.connect(":memory:")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_file_path = tmp.name
            tmp.write(ts_metadata)
            backup_conn = sqlite3.connect(temp_file_path)
            with conn:
                backup_conn.backup(conn)
            backup_conn.close()
        return conn

    def get_time_series_directory(self) -> Path | None:
        """Return the directory containing time series files.

        Returns
        -------
        Path or None
            Path to directory containing time series files or None if in-memory
        """
        return None if self._in_memory else self.directory

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: Optional[datetime] = None,
        length: Optional[int] = None,
        connection: Any = None,
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

        Returns
        -------
        TimeSeriesData
            Retrieved time series data

        Raises
        ------
        KeyError
            If the time series with the specified UUID doesn't exist
        """
        assert isinstance(metadata, SingleTimeSeriesMetadata)
        assert isinstance(metadata, SingleTimeSeriesMetadata)

        with self._get_file_handle() as f:
            root = f[self.HDF5_TS_ROOT_PATH]
            uuid = str(metadata.time_series_uuid)

            if uuid not in root:
                msg = f"Time series with {uuid=} not found"
                raise ISNotStored(msg)

            dataset = root[uuid]["data"]
            index, length = metadata.get_range(start_time=start_time, length=length)
            data = dataset[index : index + length]
            if metadata.quantity_metadata is not None:
                data = metadata.quantity_metadata.quantity_type(
                    data, metadata.quantity_metadata.units
                )

        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=data,
            normalization=metadata.normalization,
        )

    def remove_time_series(self, metadata: TimeSeriesMetadata, connection: Any = None) -> None:
        """Remove a time series array.

        Parameters
        ----------
        metadata : TimeSeriesMetadata
            Metadata for the time series to remove
        connection : Any, optional
            Optional connection parameter (not used in this implementation)

        Raises
        ------
        ISNotStored
            If the time series with the specified UUID doesn't exist
        """

        with self._get_file_handle() as f:
            root = f[self.HDF5_TS_ROOT_PATH]
            uuid = str(metadata.time_series_uuid)

            if uuid not in root:
                msg = f"Time series with {uuid=} not found"
                raise ISNotStored(msg)

            del root[uuid]

            meta_group = f[self.HDF5_TS_METADATA_ROOT_PATH]
            if uuid in meta_group:
                del meta_group[uuid]

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
        with self._get_file_handle() as f:
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

    def __enter__(self):
        """
        Enter the runtime context, opening the HDF5 file.

        Returns
        -------
        H5Manager
            The instance itself.

        Raises
        ------
        RuntimeError
            If trying to re-enter the context.
        Exception
            Propagates exceptions raised during file opening (e.g., IOError).
        """
        if self._in_context or self._file_handle:
            msg = "Cannot re-enter H5Storage context."
            raise RuntimeError(msg)
        try:
            # If the file does not exist, we want to make it?
            if self.mode in ("w", "a", "w-", "x", "r+"):
                assert self.fname
                dirname = Path(self.fname).parent
                dirname.mkdir(parents=True, exist_ok=True)

            self._file_handle = h5py.File(self.fname, self.mode)
            self._in_context = True
            return self
        except Exception:
            self._in_context = False
            self._file_handle = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, closing the file.

        Parameters
        ----------
        exc_type : type or None
            The type of the exception raised in the `with` block, if any.
        exc_val : Exception or None
            The exception instance raised in the `with` block, if any.
        exc_tb : traceback or None
            The traceback object for the exception, if any.

        Returns
        -------
        bool
            Returns False to indicate that any exception raised within the
            `with` block should be propagated.

        Notes
        -----
        Ensures the HDF5 file handle is closed and resets the context state.
        Prints a warning if an error occurs during file closing.
        """
        if self._file_handle:
            try:
                self._file_handle.close()
            except Exception as e:
                # I do not really know what type of exception could happen here.
                msg = f"Warning: Error closing HDF5 file '{self.fname}': {e}"
                logger.warning(msg)
        self._file_handle = None
        self._in_context = False
        return False

    @contextlib.contextmanager
    def _get_file_handle(self):
        """Internal context manager to provide the file handle.

        Yields the appropriate h5py.File handle. If called within the
        main `with` block (`self._in_context` is True), it yields the
        existing handle. If called outside, it opens the file in the
        specified mode, yields the handle, and ensures it's closed afterwards.

        Yields
        ------
        h5py.File
            The active HDF5 file handle.

        Raises
        ------
        RuntimeError
            If called within context but the handle is unexpectedly missing.
        Exception
            Propagates exceptions from h5py.File if opening fails outside context.
        """
        if self._in_context:
            if self._file_handle is None:
                msg = "In context but file handle is missing."
                raise RuntimeError(msg)
            yield self._file_handle
        else:
            temp_handle = None
            try:
                if self.mode in ("w", "a", "w-", "x", "r+"):
                    assert self.fname
                    dirname = Path(self.fname).parent
                    dirname.mkdir(parents=True, exist_ok=True)
                temp_handle = h5py.File(self.fname, self.mode)
                yield temp_handle
            finally:
                if temp_handle:
                    temp_handle.close()
