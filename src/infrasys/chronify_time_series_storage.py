"""Implementation of arrow storage for time series."""

import atexit
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Generator, Optional, Self
from uuid import UUID

import pandas as pd
import pint
from chronify import DatetimeRange, Store, TableSchema
from loguru import logger
from numpy.typing import NDArray
from sqlalchemy import Connection

from infrasys.exceptions import ISOperationNotAllowed
from infrasys.id_manager import IDManager
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
    TimeSeriesStorageType,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase
from infrasys.utils.path_utils import delete_if_exists


class ChronifyTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in a chronfiy database."""

    TABLE_NAME = "time_series"

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
        # Manage a mapping of UUIDs to integer IDs until we can remove UUIDs.
        self._uuid_lookup: dict[UUID, int] = uuid_lookup or {}
        self._id_manager = id_manager

    @classmethod
    def create_with_temp_directory(
        cls,
        base_directory: Optional[Path] = None,
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
    def from_file_to_tmp_file(
        cls,
        data: dict[str, Any],
        dst_dir: Optional[Path] = None,
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
        connection: Optional[Connection] = None,
    ) -> None:
        if not isinstance(time_series, SingleTimeSeries):
            msg = f"Bug: need to implement add_time_series for {type(time_series)}"
            raise NotImplementedError(msg)

        if time_series.uuid in self._uuid_lookup:
            msg = f"Bug: time series {time_series.uuid} already stored"
            raise Exception(msg)

        db_id = self._id_manager.get_next_id()
        df = self._to_dataframe(time_series, db_id)
        schema = self._make_table_schema(time_series)
        self._store.ingest_table(df, schema, connection=connection)
        self._uuid_lookup[time_series.uuid] = db_id
        logger.debug("Added {} to time series storage", time_series.summary)

    def add_raw_single_time_series(
        self, time_series_uuid: UUID, time_series_data: NDArray
    ) -> None:
        # This is problematic because chronify wants to store timestamps.
        # Should we support a mode without timestamps?
        msg = "ChronifyTimeSeriesStorage does not support add_raw_single_time_series"
        raise ISOperationNotAllowed(msg)

    @staticmethod
    def _make_table_schema(time_series: SingleTimeSeries) -> TableSchema:
        return TableSchema(
            name=ChronifyTimeSeriesStorage.TABLE_NAME,
            value_column="value",
            time_array_id_columns=["id"],
            time_config=DatetimeRange(
                start=time_series.initial_time,
                resolution=time_series.resolution,
                length=len(time_series.data),
                time_column="timestamp",
            ),
        )

    def get_engine_name(self) -> str:
        """Return the name of the underlying database engine."""
        return self._store.engine.name

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
        connection: Optional[Connection] = None,
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

    def get_raw_single_time_series(
        self, time_series_uuid: UUID, connection: Connection | None = None
    ) -> NDArray:
        db_id = self._get_db_id(time_series_uuid)
        params = (db_id,)
        query = f"""
            SELECT value
            FROM {self.TABLE_NAME}
            WHERE id = ?
            ORDER BY timestamp
        """
        df = self._store.read_query(
            self.TABLE_NAME,
            query,
            params=params,
            connection=connection,
        )
        return df["value"].values  # type: ignore

    def remove_time_series(
        self, time_series_uuid: UUID, connection: Connection | None = None
    ) -> None:
        db_id = self._get_db_id(time_series_uuid)
        self._store.delete_rows(self.TABLE_NAME, {"id": db_id}, connection=connection)

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
        connection: Optional[Connection] = None,
    ) -> SingleTimeSeries:
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
            FROM {self.TABLE_NAME}
            WHERE {where_clause}
            ORDER BY timestamp
            {limit}
        """
        df = self._store.read_query(
            self.TABLE_NAME,
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
    def open_time_series_store(self, mode: str = "r") -> Generator[Connection, None, None]:
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
