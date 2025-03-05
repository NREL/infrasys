"""Implementation of arrow storage for time series."""

import atexit
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import singledispatch
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Generator, Self
from uuid import UUID

import pandas as pd
import pint
from chronify import DatetimeRange, Store, TableSchema
from loguru import logger
from sqlalchemy import Connection

from infrasys.exceptions import ISFileExists, ISInvalidParameter
from infrasys.id_manager import IDManager
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesKey,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesKey,
    TimeSeriesMetadata,
    TimeSeriesStorageType,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase
from infrasys.utils.path_utils import delete_if_exists


_SINGLE_TIME_SERIES_BASE_NAME = "single_time_series"
_TIME_SERIES_FILENAME = "time_series_data.db"


class ChronifyTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in a chronfiy database."""

    def __init__(
        self,
        store: Store,
        id_manager: IDManager,
        read_only: bool = False,
        uuid_lookup: dict[UUID, int] | None = None,
    ) -> None:
        self._store = store
        self._read_only = read_only
        # infrasys currently uses UUIDs as unique identifies for components and time series.
        # Those will eventually use integer IDs instead.
        # We don't want to store UUIDs in the chronify database.
        # Integer IDs are much smaller and faster for search.
        # Manage a mapping of UUIDs to integer IDs until we can remove UUIDs (#80).
        self._uuid_lookup: dict[UUID, int] = uuid_lookup or {}
        self._id_manager = id_manager

    @classmethod
    def create_with_temp_directory(
        cls,
        base_directory: Path | None = None,
        engine_name: str = "duckdb",
        read_only: bool = False,
    ) -> Self:
        """Construct ChronifyTimeSeriesStorage with a temporary directory."""
        with NamedTemporaryFile(dir=base_directory, suffix=".db") as f:
            dst_file = Path(f.name)
        logger.debug("Creating database at {}", dst_file)
        atexit.register(delete_if_exists, dst_file)
        store = Store(engine_name=engine_name, file_path=dst_file)
        id_manager = IDManager(next_id=1)
        return cls(store, id_manager, read_only=read_only)

    @classmethod
    def create_with_permanent_directory(
        cls,
        base_directory: Path,
        engine_name: str = "duckdb",
        read_only: bool = False,
    ) -> Self:
        """Construct ChronifyTimeSeriesStorage with a permanent directory."""
        dst_file = base_directory / _TIME_SERIES_FILENAME
        if dst_file.exists():
            msg = f"time series database already exists: {dst_file}"
            raise ISFileExists(msg)
        logger.debug("Creating database at {}", dst_file)
        store = Store(engine_name=engine_name, file_path=dst_file)
        id_manager = IDManager(next_id=1)
        return cls(store, id_manager, read_only=read_only)

    @classmethod
    def from_file_to_tmp_file(
        cls,
        data: dict[str, Any],
        dst_dir: Path | None = None,
        read_only: bool = False,
    ) -> Self:
        """Construct ChronifyTimeSeriesStorage after copying from an existing database file."""
        id_manager, uuid_lookup = cls._deserialize_ids(data)
        with NamedTemporaryFile(dir=dst_dir, suffix=".db") as f:
            dst_file = Path(f.name)
        orig_store = Store(engine_name=data["engine_name"], file_path=data["filename"])
        orig_store.backup(dst_file)
        new_store = Store(engine_name=data["engine_name"], file_path=dst_file)
        atexit.register(delete_if_exists, dst_file)
        return cls(new_store, id_manager, read_only=read_only, uuid_lookup=uuid_lookup)

    @classmethod
    def from_file(cls, data: dict[str, Any], read_only: bool = False) -> Self:
        """Construct ChronifyTimeSeriesStorage with an existing database file."""
        id_manager, uuid_lookup = cls._deserialize_ids(data)
        store = Store(engine_name=data["engine_name"], file_path=Path(data["filename"]))
        return cls(store, id_manager, read_only=read_only, uuid_lookup=uuid_lookup)

    @staticmethod
    def _deserialize_ids(data: dict[str, Any]) -> tuple[IDManager, dict[UUID, int]]:
        uuid_lookup: dict[UUID, int] = {}
        max_id = 0
        for key, val in data["uuid_lookup"].items():
            uuid_lookup[UUID(key)] = val
            if val > max_id:
                max_id = val
        id_manager = IDManager(next_id=max_id + 1)
        return id_manager, uuid_lookup

    def get_database_url(self) -> str:
        """Return the path to the underlying database."""
        assert self._store.engine.url.database is not None
        # We don't expect to use an in-memory db.
        return self._store.engine.url.database

    def get_time_series_directory(self) -> Path:
        assert self._store.engine.url.database is not None
        return Path(self._store.engine.url.database).parent

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
        connection: Connection | None = None,
    ) -> None:
        if not isinstance(time_series, SingleTimeSeries):
            msg = f"Bug: need to implement add_time_series for {type(time_series)}"
            raise NotImplementedError(msg)

        if time_series.uuid in self._uuid_lookup:
            msg = f"Bug: time series {time_series.uuid} already stored"
            raise Exception(msg)

        db_id = self._id_manager.get_next_id()
        df = self._to_dataframe(time_series, db_id)
        schema = _make_table_schema(time_series, _get_table_name(time_series))
        # There is no reason to run time checks because we are generating the timestamps
        # from initial_time, resolution, and length, so they are guaranteed to be correct.
        self._store.ingest_table(df, schema, connection=connection, skip_time_checks=False)
        self._uuid_lookup[time_series.uuid] = db_id
        logger.debug("Added {} to time series storage", time_series.summary)

    def check_timestamps(self, key: TimeSeriesKey, connection: Connection | None = None) -> None:
        table_name = _get_table_name(key)
        self._store.check_timestamps(table_name, connection=connection)

    def get_engine_name(self) -> str:
        """Return the name of the underlying database engine."""
        return self._store.engine.name

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: Connection | None = None,
    ) -> Any:
        if isinstance(metadata, SingleTimeSeriesMetadata):
            return self._get_single_time_series(
                metadata=metadata,
                start_time=start_time,
                length=length,
                connection=connection,
            )

        msg = f"Bug: need to implement get_time_series for {type(metadata)}"
        raise NotImplementedError(msg)

    def remove_time_series(
        self, metadata: TimeSeriesMetadata, connection: Connection | None = None
    ) -> None:
        db_id = self._get_db_id(metadata.time_series_uuid)
        table_name = _get_table_name(metadata)
        num_deleted = self._store.delete_rows(table_name, {"id": db_id}, connection=connection)
        if num_deleted < 1:
            msg = f"Failed to delete rows in the chronfiy database for {metadata.time_series_uuid}"
            raise ISInvalidParameter(msg)

    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Path | str | None = None
    ) -> None:
        ts_dir = dst if isinstance(dst, Path) else Path(dst)
        path = ts_dir / "time_series_data.db"
        assert not path.exists(), path
        self._store.backup(path)
        data["filename"] = str(path)
        data["time_series_storage_type"] = TimeSeriesStorageType.CHRONIFY.value
        data["engine_name"] = self._store.engine.name
        data["uuid_lookup"] = {str(k): v for k, v in self._uuid_lookup.items()}

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: Connection | None = None,
    ) -> SingleTimeSeries:
        table_name = _get_table_name(metadata)
        db_id = self._get_db_id(metadata.time_series_uuid)
        _, required_len = metadata.get_range(start_time=start_time, length=length)
        where_clauses = ["id = ?"]
        params: list[Any] = [db_id]
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        where_clause = " AND ".join(where_clauses)
        limit = "" if length is None else f" LIMIT {required_len}"
        query = f"""
            SELECT timestamp, value
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY timestamp
            {limit}
        """
        df = self._store.read_query(
            table_name,
            query,
            params=tuple(params),
            connection=connection,
        )
        if len(df) != required_len:
            msg = f"Bug: {len(df)=} {length=} {required_len=}"
            raise Exception(msg)
        values = df["value"].values
        if metadata.quantity_metadata is not None:
            np_array = metadata.quantity_metadata.quantity_type(
                values, metadata.quantity_metadata.units
            )
        else:
            np_array = values
        return SingleTimeSeries(
            uuid=metadata.time_series_uuid,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=np_array,
            normalization=metadata.normalization,
        )

    @contextmanager
    def open_time_series_store(self) -> Generator[Connection, None, None]:
        with self._store.engine.begin() as conn:
            yield conn

    def _to_dataframe(self, time_series: SingleTimeSeries, db_id: int) -> pd.DataFrame:
        if isinstance(time_series.data, pint.Quantity):
            array = time_series.data.magnitude
        else:
            array = time_series.data
        df = pd.DataFrame({"timestamp": time_series.make_timestamps(), "value": array})
        df["id"] = db_id
        return df

    def _get_db_id(self, time_series_uuid: UUID) -> int:
        db_id = self._uuid_lookup.get(time_series_uuid)
        if db_id is None:
            msg = f"Bug: time series {time_series_uuid} not stored"
            raise Exception(msg)
        return db_id


@singledispatch
def _get_table_name(time_series) -> str:
    msg = f"Bug: {type(time_series)}"
    raise NotImplementedError(msg)


@_get_table_name.register(SingleTimeSeries)
def _(time_series) -> str:
    return _get_single_time_series_table_name(
        time_series.initial_time, time_series.resolution, time_series.length
    )


@_get_table_name.register(SingleTimeSeriesMetadata)
def _(metadata) -> str:
    return _get_single_time_series_table_name(
        metadata.initial_time, metadata.resolution, metadata.length
    )


@_get_table_name.register(SingleTimeSeriesKey)
def _(key) -> str:
    return _get_single_time_series_table_name(key.initial_time, key.resolution, key.length)


def _get_single_time_series_table_name(
    initial_time: datetime,
    resolution: timedelta,
    length: int,
) -> str:
    return "_".join(
        (
            _SINGLE_TIME_SERIES_BASE_NAME,
            initial_time.isoformat().replace("-", "_").replace(":", "_"),
            str(resolution.seconds),
            str(length),
        )
    )


@singledispatch
def _get_table_base_name(time_series) -> str:
    msg = "Bug: need to implement _get_table_base_name for {type(time_series)}"
    raise NotImplementedError(msg)


@_get_table_base_name.register(SingleTimeSeries)
def _(time_series: SingleTimeSeries) -> str:
    return _SINGLE_TIME_SERIES_BASE_NAME


@singledispatch
def _make_time_config(time_series) -> Any:
    msg = "Bug: need to implement _make_time_config for {type(time_series)}"
    raise NotImplementedError(msg)


@_make_time_config.register(SingleTimeSeries)
def _(time_series: SingleTimeSeries) -> DatetimeRange:
    return DatetimeRange(
        start=time_series.initial_time,
        resolution=time_series.resolution,
        length=len(time_series.data),
        time_column="timestamp",
    )


def _make_table_schema(time_series: TimeSeriesData, table_name: str) -> TableSchema:
    return TableSchema(
        name=table_name,
        value_column="value",
        time_array_id_columns=["id"],
        time_config=_make_time_config(time_series),
    )
