import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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

TIME_SERIES_STORAGE_FILE = "time_series_storage.h5"
HDF5_TS_ROOT_PATH = "time_series"
HDF5_TS_METADATA_ROOT_PATH = "time_series_metadata"
TIME_SERIES_DATA_FORMAT_VERSION = "1.0.0"
TIME_SERIES_VERSION_KEY = "data_format_version"


class HDF5TimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in an h5 file."""

    def __init__(
        self,
        directory: Path | None = None,
        time_series_storage_file: Path | str | None = None,
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
        self._ts_directory = directory
        self._in_memory = in_memory
        self._file_path = time_series_storage_file

        if in_memory:
            self._f = h5py.File.in_memory()
        else:
            self._f = h5py.File(self._file_path, "a")

        if HDF5_TS_ROOT_PATH not in self._f:
            root = self._f.create_group(HDF5_TS_ROOT_PATH)
            root.attrs[TIME_SERIES_VERSION_KEY] = TIME_SERIES_DATA_FORMAT_VERSION

        if HDF5_TS_METADATA_ROOT_PATH not in self._f:
            self._f.create_group(HDF5_TS_METADATA_ROOT_PATH)

    def _get_root(self):
        """Get the root group for time series data.

        Returns
        -------
        h5py.Group
            Root group for time series data
        """
        return self._f[HDF5_TS_ROOT_PATH]

    def _serialize_compression_settings(self):
        """Add default compression settings."""
        root = self._get_root()
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
        root = self._get_root()
        uuid = str(metadata.time_series_uuid)

        if uuid not in root:
            group = root.create_group(uuid)

            group.create_dataset(
                "data", data=time_series.data, compression="gzip", compression_opts=5
            )

            group.attrs["type"] = metadata.type
            group.attrs["initial_timestamp"] = metadata.initial_time.isoformat()
            group.attrs["resolution"] = metadata.resolution.total_seconds()

    def get_metadata_store(self) -> sqlite3.Connection:
        """Get the metadata store.

        Returns
        -------
        TimeSeriesMetadataStore
            The metadata store
        """
        ts_metadata = bytes(self._f[HDF5_TS_METADATA_ROOT_PATH][:])
        conn = sqlite3.connect(":memory:")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_file_path = tmp.name
            tmp.write(ts_metadata)
            backup_conn = sqlite3.connect(temp_file_path)
            with conn:
                backup_conn.backup(conn)
            backup_conn.close()
        return conn

    def get_time_series_directory(self) -> Optional[Path]:
        """Return the directory containing time series files.

        Returns
        -------
        Path or None
            Path to directory containing time series files or None if in-memory
        """
        return None if self._in_memory else self._ts_directory

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
        root = self._get_root()
        uuid = str(metadata.time_series_uuid)

        if uuid not in root:
            msg = f"Time series with {uuid=} not found"
            raise ISNotStored(msg)

        dataset = root[uuid]["data"]
        index, length = metadata.get_range(start_time=start_time, length=length)
        data = dataset[index : index + length]
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
        root = self._get_root()
        uuid = str(metadata.time_series_uuid)

        if uuid not in root:
            msg = f"Time series with {uuid=} not found"
            raise ISNotStored(msg)

        del root[uuid]

        meta_group = self._f[HDF5_TS_METADATA_ROOT_PATH]
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
        dst_path = Path(dst) / TIME_SERIES_STORAGE_FILE if Path(dst).is_dir() else Path(dst)
        self._f.flush()

        # NOTE: I need to fix this
        # with open(metadata_fpath, "rb") as f:
        #     binary_data = f.read()

        with h5py.File(dst_path, "a") as dst_file:
            if HDF5_TS_ROOT_PATH in self._f:
                h5py.h5o.copy(
                    self._f.id,
                    HDF5_TS_ROOT_PATH.encode("utf-8"),
                    dst_file.id,
                    HDF5_TS_ROOT_PATH.encode("utf-8"),
                )
                if HDF5_TS_METADATA_ROOT_PATH in dst_file:
                    del dst_file[HDF5_TS_METADATA_ROOT_PATH]
                # dst_file.create_dataset(
                #     HDF5_TS_METADATA_ROOT_PATH,
                #     data=np.frombuffer(binary_data, dtype=np.uint8),
                #     dtype=np.uint8,
                # )

        self.add_serialized_data(data)

    def __del__(self):
        """Cleanup when the object is garbage collected."""
        if hasattr(self, "_f") and self._f:
            self._f.close()
