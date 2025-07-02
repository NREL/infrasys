"""Manages time series arrays"""

from contextlib import contextmanager
import sqlite3
from datetime import datetime
from functools import singledispatch
from pathlib import Path
from typing import Any, Generator, Optional, Type

from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys import Component
from infrasys.exceptions import ISInvalidParameter, ISOperationNotAllowed
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.time_series_metadata_store import TimeSeriesMetadataStore
from infrasys.time_series_models import (
    DatabaseConnection,
    SingleTimeSeries,
    SingleTimeSeriesKey,
    SingleTimeSeriesMetadata,
    NonSequentialTimeSeries,
    NonSequentialTimeSeriesMetadata,
    NonSequentialTimeSeriesKey,
    TimeSeriesData,
    TimeSeriesKey,
    TimeSeriesMetadata,
    TimeSeriesStorageType,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

try:
    from infrasys.chronify_time_series_storage import ChronifyTimeSeriesStorage

    is_chronify_installed = True
except ImportError:
    is_chronify_installed = False


TIME_SERIES_KWARGS = {
    "time_series_read_only": False,
    "time_series_directory": None,
    "time_series_storage_type": TimeSeriesStorageType.ARROW,
    "chronify_engine_name": "duckdb",
}


def _process_time_series_kwarg(key: str, **kwargs: Any) -> Any:
    return kwargs.get(key, TIME_SERIES_KWARGS[key])


class TimeSeriesManager:
    """Manages time series for a system."""

    def __init__(
        self,
        con: sqlite3.Connection,
        storage: Optional[TimeSeriesStorageBase] = None,
        initialize: bool = True,
        **kwargs,
    ) -> None:
        self._con = con
        self._metadata_store = TimeSeriesMetadataStore(con, initialize=initialize)
        self._read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        self._storage = storage or self.create_new_storage(**kwargs)

        # TODO: create parsing mechanism? CSV, CSV + JSON

    @staticmethod
    def create_new_storage(permanent: bool = False, **kwargs):
        base_directory: Path | None = _process_time_series_kwarg("time_series_directory", **kwargs)
        storage_type = _process_time_series_kwarg("time_series_storage_type", **kwargs)
        if permanent:
            if base_directory is None:
                msg = "Can't convert to permanent storage without a base directory"
                raise ISInvalidParameter(msg)

        match storage_type:
            case TimeSeriesStorageType.ARROW:
                if permanent:
                    assert base_directory is not None
                    return ArrowTimeSeriesStorage.create_with_permanent_directory(base_directory)
                return ArrowTimeSeriesStorage.create_with_temp_directory(
                    base_directory=base_directory
                )
            case TimeSeriesStorageType.CHRONIFY:
                if not is_chronify_installed:
                    msg = (
                        "chronify is not installed. Please choose a different time series storage "
                        'option or install chronify with `pip install "infrasys[chronify]"`.'
                    )
                    raise ImportError(msg)
                if permanent:
                    assert base_directory is not None
                    return ChronifyTimeSeriesStorage.create_with_permanent_directory(
                        base_directory,
                        engine_name=_process_time_series_kwarg("chronify_engine_name", **kwargs),
                        read_only=_process_time_series_kwarg("time_series_read_only", **kwargs),
                    )
                return ChronifyTimeSeriesStorage.create_with_temp_directory(
                    base_directory=base_directory,
                    engine_name=_process_time_series_kwarg("chronify_engine_name", **kwargs),
                    read_only=_process_time_series_kwarg("time_series_read_only", **kwargs),
                )
            case TimeSeriesStorageType.MEMORY:
                return InMemoryTimeSeriesStorage()
            case _:
                msg = f"{storage_type=}"
                raise NotImplementedError(msg)

    @property
    def metadata_store(self) -> TimeSeriesMetadataStore:
        """Return the time series metadata store."""
        return self._metadata_store

    @property
    def storage(self) -> TimeSeriesStorageBase:
        """Return the time series storage object."""
        return self._storage

    def add(
        self,
        time_series: TimeSeriesData,
        *owners: Component | SupplementalAttribute,
        connection: DatabaseConnection | None = None,
        **user_attributes: Any,
    ) -> TimeSeriesKey:
        """Store a time series array for one or more components or supplemental attributes.

        Parameters
        ----------
        time_series : TimeSeriesData
            Time series data to store.
        owners : Component | SupplementalAttribute
            Add the time series to all of these components or supplemental attributes.
        connection
            Optional connection to use for the operation.
        user_attributes : Any
            Key/value pairs to store with the time series data. Must be JSON-serializable.

        Raises
        ------
        ISAlreadyAttached
            Raised if the variable name and user attributes match any time series already
            attached to one of the components or supplemental attributes.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        self._handle_read_only()
        if not owners:
            msg = "add_time_series requires at least one component or supplemental attribute"
            raise ISOperationNotAllowed(msg)

        ts_type = type(time_series)
        if not issubclass(ts_type, TimeSeriesData):
            msg = f"The first argument must be an instance of TimeSeriesData: {ts_type}"
            raise ValueError(msg)
        metadata_type = ts_type.get_time_series_metadata_type()
        metadata = metadata_type.from_data(time_series, **user_attributes)

        data_is_stored = self._metadata_store.has_time_series(time_series.uuid)
        # Call this first because it could raise an exception.
        self._metadata_store.add(
            metadata, *owners, connection=_get_metadata_connection(connection)
        )
        if not data_is_stored:
            self._storage.add_time_series(
                metadata,
                time_series,
                connection=_get_data_connection(connection),
            )
        return make_time_series_key(metadata)

    def get(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: DatabaseConnection | None = None,
        **user_attributes,
    ) -> TimeSeriesData:
        """Return a time series array.

        Raises
        ------
        ISNotStored
            Raised if no time series matches the inputs.
            Raised if the inputs match more than one time series.
        ISOperationNotAllowed
            Raised if the inputs match more than one time series.

        See Also
        --------
        list_time_series
        """
        metadata = self._metadata_store.get_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__ if time_series_type else None,
            **user_attributes,
        )
        return self._get_by_metadata(
            metadata, start_time=start_time, length=length, connection=connection
        )

    def get_by_key(
        self,
        owner: Component | SupplementalAttribute,
        key: TimeSeriesKey,
        connection: DatabaseConnection | None = None,
    ) -> TimeSeriesData:
        """Return a time series array by key."""
        metadata = self._metadata_store.get_metadata(
            owner,
            variable_name=key.variable_name,
            time_series_type=key.time_series_type.__name__,
            **key.user_attributes,
        )
        return self._get_by_metadata(metadata, connection=connection)

    def has_time_series(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        **user_attributes,
    ) -> bool:
        """Return True if the component or supplemental atttribute has time series matching the
        inputs.
        """
        return self._metadata_store.has_time_series_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__ if time_series_type else None,
            **user_attributes,
        )

    def list_time_series(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: DatabaseConnection | None = None,
        **user_attributes: Any,
    ) -> list[TimeSeriesData]:
        """Return all time series that match the inputs."""
        metadata = self.list_time_series_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )
        return [
            self._get_by_metadata(x, start_time=start_time, length=length, connection=connection)
            for x in metadata
        ]

    def list_time_series_keys(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        **user_attributes: Any,
    ) -> list[TimeSeriesKey]:
        """Return all time series keys that match the inputs."""
        return [
            make_time_series_key(x)
            for x in self.list_time_series_metadata(
                owner, variable_name, time_series_type, **user_attributes
            )
        ]

    def list_time_series_metadata(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        **user_attributes: Any,
    ) -> list[TimeSeriesMetadata]:
        """Return all time series metadata that match the inputs."""
        return self._metadata_store.list_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__ if time_series_type else None,
            **user_attributes,
        )

    def remove(
        self,
        *owners: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] | None = None,
        connection: DatabaseConnection | None = None,
        **user_attributes: Any,
    ):
        """Remove all time series arrays matching the inputs.

        Raises
        ------
        ISNotStored
            Raised if no time series match the inputs.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        self._handle_read_only()
        metadata = self._metadata_store.remove(
            *owners,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__ if time_series_type else None,
            connection=_get_metadata_connection(connection),
            **user_attributes,
        )
        time_series = {x.time_series_uuid: x for x in metadata}
        missing_uuids = self._metadata_store.list_missing_time_series(time_series.keys())
        for uuid in missing_uuids:
            self._storage.remove_time_series(
                time_series[uuid], connection=_get_data_connection(connection)
            )
            logger.info("Removed time series {}", uuid)

    def copy(
        self,
        dst: Component | SupplementalAttribute,
        src: Component | SupplementalAttribute,
        name_mapping: dict[str, str] | None = None,
    ) -> None:
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst
            Destination component or supplemental attribute
        src
            Source component or supplemental attribute
        name_mapping : dict[str, str]
            Optionally map src names to different dst names.
            If provided and src has a time_series with a name not present in name_mapping, that
            time_series will not copied. If name_mapping is nothing then all time_series will be
            copied with src's names.

        Notes
        -----
        name_mapping is currently not implemented.
        """
        self._handle_read_only()
        raise NotImplementedError

    def _get_by_metadata(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: DatabaseConnection | None = None,
    ) -> TimeSeriesData:
        return self._storage.get_time_series(
            metadata,
            start_time=start_time,
            length=length,
            connection=_get_data_connection(connection),
        )

    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Optional[Path | str] = None
    ) -> None:
        """Serialize the time series data to dst."""
        if isinstance(self._storage, InMemoryTimeSeriesStorage):
            new_storage = self.convert_storage(
                time_series_storage_type=TimeSeriesStorageType.ARROW,
                time_series_directory=dst,
                in_place=False,
                permanent=True,
            )
            assert isinstance(new_storage, ArrowTimeSeriesStorage)
            new_storage.add_serialized_data(data)
        else:
            self._storage.serialize(data, dst, src=src)

    @classmethod
    def deserialize(
        cls,
        con: sqlite3.Connection,
        data: dict[str, Any],
        parent_dir: Path | str,
        **kwargs: Any,
    ) -> "TimeSeriesManager":
        """Deserialize the class. Must also call add_reference_counts after deserializing
        components.
        """
        if (
            _process_time_series_kwarg("time_series_storage_type", **kwargs)
            == TimeSeriesStorageType.MEMORY
        ):
            msg = "De-serialization does not support in-memory time series storage."
            raise ISOperationNotAllowed(msg)

        dst_time_series_directory = _process_time_series_kwarg("time_series_directory", **kwargs)
        if dst_time_series_directory is not None and not Path(dst_time_series_directory).exists():
            msg = f"time_series_directory={dst_time_series_directory} does not exist"
            raise FileNotFoundError(msg)
        read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        time_series_dir = Path(parent_dir) / data["directory"]
        storage: TimeSeriesStorageBase

        # This term was introduced in v0.3.0. Maintain compatibility with old serialized files.
        ts_type = data.get("time_series_storage_type", TimeSeriesStorageType.ARROW)
        match ts_type:
            case TimeSeriesStorageType.CHRONIFY:
                if not is_chronify_installed:
                    msg = (
                        "This system used chronify to manage time series data but the package is "
                        'not installed. Please install it with `pip install "infrasys[chronify]"`.'
                    )
                    raise ImportError(msg)
                if read_only:
                    storage = ChronifyTimeSeriesStorage.from_file(
                        data,
                        read_only=True,
                    )
                else:
                    storage = ChronifyTimeSeriesStorage.from_file_to_tmp_file(
                        data,
                        dst_dir=dst_time_series_directory,
                        read_only=read_only,
                    )
            case TimeSeriesStorageType.ARROW:
                if read_only:
                    storage = ArrowTimeSeriesStorage.create_with_permanent_directory(
                        time_series_dir
                    )
                else:
                    storage = ArrowTimeSeriesStorage.create_with_temp_directory(
                        base_directory=dst_time_series_directory
                    )
                    storage.serialize({}, storage.get_time_series_directory(), src=time_series_dir)
            case _:
                msg = f"time_series_storage_type={ts_type} is not supported"
                raise NotImplementedError(msg)

        mgr = cls(con, storage=storage, initialize=False, **kwargs)
        if (
            "time_series_storage_type" in kwargs
            and _process_time_series_kwarg("time_series_storage_type", **kwargs) != ts_type
        ):
            mgr.convert_storage(**kwargs)
        return mgr

    @contextmanager
    def open_time_series_store(self) -> Generator[DatabaseConnection, None, None]:
        """Open a connection to the time series metadata and data stores."""
        with self._storage.open_time_series_store() as data_conn:
            try:
                yield DatabaseConnection(metadata_conn=self._con, data_conn=data_conn)
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise

    def _handle_read_only(self) -> None:
        if self._read_only:
            msg = "Cannot modify time series in read-only mode."
            raise ISOperationNotAllowed(msg)

    def convert_storage(self, in_place: bool = True, **kwargs) -> TimeSeriesStorageBase:
        """
        Create a new storage instance and copy all time series from the current to new storage.

        Parameters
        ----------
        in_place : bool
            If True, replace the current storage with the new storage.

        Returns
        -------
        TimeSeriesStorageBase
            The new storage instance.
        """
        new_storage = self.create_new_storage(**kwargs)
        for time_series_type in (SingleTimeSeries, NonSequentialTimeSeries):
            for time_series_uuid in self.metadata_store.unique_uuids_by_type(
                time_series_type.__name__
            ):
                metadata = self.metadata_store.list_metadata_with_time_series_uuid(
                    time_series_uuid, limit=1
                )
                if len(metadata) != 1:
                    msg = f"Expected 1 metadata for {time_series_uuid}, got {len(metadata)}"
                    raise Exception(msg)

                time_series = self._storage.get_time_series(metadata[0])
                new_storage.add_time_series(metadata[0], time_series)

        if in_place:
            self._storage = new_storage

        return new_storage


@singledispatch
def make_time_series_key(metadata) -> TimeSeriesKey:
    msg = f"make_time_series_keys not implemented for {type(metadata)}"
    raise NotImplementedError(msg)


@make_time_series_key.register(SingleTimeSeriesMetadata)
def _(metadata: SingleTimeSeriesMetadata) -> TimeSeriesKey:
    return SingleTimeSeriesKey(
        initial_time=metadata.initial_time,
        resolution=metadata.resolution,
        length=metadata.length,
        user_attributes=metadata.user_attributes,
        variable_name=metadata.variable_name,
        time_series_type=SingleTimeSeries,
    )


@make_time_series_key.register(NonSequentialTimeSeriesMetadata)
def _(metadata: NonSequentialTimeSeriesMetadata) -> TimeSeriesKey:
    return NonSequentialTimeSeriesKey(
        length=metadata.length,
        user_attributes=metadata.user_attributes,
        variable_name=metadata.variable_name,
        time_series_type=NonSequentialTimeSeries,
    )


def _get_data_connection(conn: DatabaseConnection | None) -> Any:
    return None if conn is None else conn.data_conn


def _get_metadata_connection(conn: DatabaseConnection | None) -> sqlite3.Connection | None:
    return None if conn is None else conn.metadata_conn
