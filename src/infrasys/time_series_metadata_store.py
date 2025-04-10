"""Stores time series metadata in a SQLite database."""

import hashlib
import itertools
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from uuid import UUID

from loguru import logger

from infrasys import (
    KEY_VALUE_STORE_TABLE,
    TIME_SERIES_ASSOCIATIONS_TABLE,
    TIME_SERIES_METADATA_TABLE,
    Component,
    __version__,
)
from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.serialization import (
    TYPE_METADATA,
    SerializedTypeMetadata,
    deserialize_value,
    serialize_value,
)
from infrasys.supplemental_attribute_manager import SupplementalAttribute
from infrasys.time_series_models import (
    NonSequentialTimeSeriesMetadataBase,
    SingleTimeSeriesMetadataBase,
    TimeSeriesMetadata,
)
from infrasys.utils.sqlite import execute
from infrasys.utils.time_utils import to_iso_8601


class TimeSeriesMetadataStore:
    """Stores time series metadata in a SQLite database."""

    def __init__(self, con: sqlite3.Connection, initialize: bool = True):
        self._con = con
        if initialize:
            self._create_associations_table()
            self._create_key_value_store()
        self._cache_metadata: dict[UUID, TimeSeriesMetadata] = {}

    def _load_metadata_into_memory(self):
        query = f"SELECT json(metadata) FROM {TIME_SERIES_METADATA_TABLE}"
        cursor = self._con.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            metadata = _deserialize_time_series_metadata(row[0])
            self._cache_metadata[metadata.uuid] = metadata
        cursor.execute(f"DROP TABLE {TIME_SERIES_METADATA_TABLE}")
        self._con.commit()

    def _create_key_value_store(self):
        schema = ["key TEXT PRIMARY KEY", "value JSON NOT NULL"]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {KEY_VALUE_STORE_TABLE}({schema_text})")
        self._create_indexes(cur)

        rows = [("infrasys_version", __version__)]
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {KEY_VALUE_STORE_TABLE}(key, value) VALUES({placeholder})"
        cur.executemany(query, rows)
        self._con.commit()
        logger.debug("Created metadata table")

    def _create_metadata_table(self):
        schema = [
            "id INTEGER PRIMARY KEY",
            "metadata_uuid TEXT NOT NULL",
            "metadata JSON TEXT NOT NULL",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {TIME_SERIES_METADATA_TABLE}({schema_text})")
        self._con.commit()
        logger.debug("Created time series medatadata table")

    def _create_associations_table(self):
        schema = [
            "id INTEGER PRIMARY KEY",
            "time_series_uuid TEXT NOT NULL",
            "time_series_type TEXT NOT NULL",
            "time_series_category TEXT NOT NULL",
            "initial_timestamp TEXT",
            "resolution TEXT NULL",
            "horizon TEXT",
            "interval TEXT",
            "window_count INTEGER",
            "length INTEGER",
            "name TEXT NOT NULL",
            "owner_uuid TEXT NOT NULL",
            "owner_type TEXT NOT NULL",
            "owner_category TEXT NOT NULL",
            "features TEXT NOT NULL",
            "metadata_uuid TEXT NOT NULL",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {TIME_SERIES_ASSOCIATIONS_TABLE}({schema_text})")
        self._create_indexes(cur)
        self._con.commit()
        logger.debug("Created time series associations table")

    def _create_indexes(self, cur) -> None:
        # Index strategy:
        # 1. Optimize for these user queries with indexes:
        #    1a. all time series attached to one component
        #    1b. time series for one component + variable_name + type
        #    1c. time series for one component with all user attributes
        # 2. Optimize for checks at system.add_time_series. Use all fields and attribute hash.
        # 3. Optimize for returning all metadata for a time series UUID.
        execute(
            cur,
            f"CREATE INDEX IF NOT EXISTS by_c_vn_tst_hash ON {TIME_SERIES_ASSOCIATIONS_TABLE} "
            f"(owner_uuid, time_series_type, name, resolution, features)",
        )
        execute(
            cur,
            f"CREATE INDEX IF NOT EXISTS by_ts_uuid ON {TIME_SERIES_ASSOCIATIONS_TABLE} (time_series_uuid)",
        )

    def add(
        self,
        metadata: TimeSeriesMetadata,
        *owners: Component | SupplementalAttribute,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        """Add metadata to the store.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series metadata already stored.
        """
        where_clause, params = self._make_where_clause(
            owners,
            metadata.variable_name,
            metadata.type,
            **metadata.features,
        )

        con = connection or self._con
        cur = con.cursor()
        query = f"SELECT 1 FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        res = execute(cur, query, params=params).fetchone()
        if res:
            msg = f"Time series with {metadata=} is already stored."
            raise ISAlreadyAttached(msg)

        if isinstance(metadata, SingleTimeSeriesMetadataBase):
            resolution = to_iso_8601(metadata.resolution)
            initial_time = str(metadata.initial_time)
            horizon = None
            interval = None
            window_count = None
            time_series_category = "StaticTimeSeries"
        elif isinstance(metadata, NonSequentialTimeSeriesMetadataBase):
            resolution = None
            initial_time = None
            horizon = None
            interval = None
            time_series_category = "NonSequentialTimeSeries"
            window_count = None
        else:
            raise NotImplementedError

        rows = [
            (
                None,  # auto-assigned by sqlite
                str(metadata.time_series_uuid),
                metadata.type,
                time_series_category,
                initial_time,
                resolution,
                horizon,
                interval,
                window_count,
                metadata.length if hasattr(metadata, "length") else None,
                metadata.variable_name,
                str(owner.uuid),
                owner.__class__.__name__,
                "Component",
                json.dumps(metadata.features),
                str(metadata.uuid),
            )
            for owner in owners
        ]
        self._insert_rows(rows, cur)
        if connection is None:
            self._con.commit()

        self._cache_metadata[metadata.uuid] = metadata
        # else, commit/rollback will occur at a higer level.

    def get_time_series_counts(self) -> "TimeSeriesCounts":
        """Return summary counts of components and time series."""
        query = f"""
            SELECT
                owner_type
                ,time_series_type
                ,initial_timestamp
                ,resolution
                ,count(*) AS count
            FROM {TIME_SERIES_ASSOCIATIONS_TABLE}
            GROUP BY
                owner_type
                ,time_series_type
                ,initial_timestamp
                ,resolution
            ORDER BY
                owner_type
                ,time_series_type
                ,initial_timestamp
                ,resolution
        """
        cur = self._con.cursor()
        rows = execute(cur, query).fetchall()
        time_series_type_count = {(x[0], x[1], x[2], x[3]): x[4] for x in rows}

        time_series_count = execute(
            cur, f"SELECT COUNT(DISTINCT time_series_uuid) from {TIME_SERIES_ASSOCIATIONS_TABLE}"
        ).fetchall()[0][0]

        return TimeSeriesCounts(
            time_series_count=time_series_count,
            time_series_type_count=time_series_type_count,
        )

    def get_metadata(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features,
    ) -> TimeSeriesMetadata:
        """Return the metadata matching the inputs.

        Raises
        ------
        ISOperationNotAllowed
            Raised if more than one metadata instance matches the inputs.
        """
        metadata_list = self.list_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **features,
        )
        if not metadata_list:
            msg = "No time series matching the inputs is stored"
            raise ISNotStored(msg)

        if len(metadata_list) > 1:
            msg = f"Found more than metadata matching inputs: {len(metadata_list)}"
            raise ISOperationNotAllowed(msg)

        return metadata_list[0]

    def has_time_series(self, time_series_uuid: UUID) -> bool:
        """Return True if there is time series matching the UUID."""
        cur = self._con.cursor()
        query = f"SELECT 1 FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE time_series_uuid = ?"
        row = execute(cur, query, params=(str(time_series_uuid),)).fetchone()
        return row

    def has_time_series_metadata(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features: Any,
    ) -> bool:
        """Return True if there is time series metadata matching the inputs."""
        where_clause, params = self._make_where_clause(
            (owner,), variable_name, time_series_type, **features
        )
        query = f"SELECT 1 FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        cur = self._con.cursor()
        res = execute(cur, query, params=params).fetchone()
        return bool(res)

    def list_existing_time_series(self, time_series_uuids: Iterable[UUID]) -> set[UUID]:
        """Return the UUIDs that are present."""
        cur = self._con.cursor()
        params = tuple(str(x) for x in time_series_uuids)
        uuids = ",".join(itertools.repeat("?", len(params)))
        query = f"SELECT time_series_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE time_series_uuid IN ({uuids})"
        rows = execute(cur, query, params=params).fetchall()
        return {UUID(x[0]) for x in rows}

    def list_missing_time_series(self, time_series_uuids: Iterable[UUID]) -> set[UUID]:
        """Return the UUIDs that are not present."""
        existing_uuids = set(self.list_existing_time_series(time_series_uuids))
        return set(time_series_uuids) - existing_uuids

    def list_metadata(
        self,
        *owners: Component | SupplementalAttribute,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features,
    ) -> list[TimeSeriesMetadata]:
        """Return a list of metadata that match the query."""
        where_clause, params = self._make_where_clause(
            owners, variable_name, time_series_type, **features
        )
        query = f"SELECT metadata_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        metadata_uuids = [UUID(row[0]) for row in rows]
        return [
            self._cache_metadata[uuid] for uuid in metadata_uuids if uuid in self._cache_metadata
        ]

    def list_metadata_with_time_series_uuid(
        self, time_series_uuid: UUID, limit: int | None = None
    ) -> list[TimeSeriesMetadata]:
        """Return metadata attached to the given time_series_uuid.

        Parameters
        ----------
        time_series_uuid
            The UUID of the time series.
        limit
            The maximum number of metadata to return. If None, all metadata are returned.
        """
        params = (str(time_series_uuid),)
        limit_str = "" if limit is None else f"LIMIT {limit}"
        # Use the denormalized view
        query = f"""
        SELECT
            metadata_uuid
        FROM {TIME_SERIES_ASSOCIATIONS_TABLE}
        WHERE
            time_series_uuid = ? {limit_str}
        """
        cur = self._con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        return [
            self._cache_metadata[UUID(x[0])] for x in rows if UUID(x[0]) in self._cache_metadata
        ]

    def list_rows(
        self,
        *components: Component | SupplementalAttribute,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        columns=None,
        **features,
    ) -> list[tuple]:
        """Return a list of rows that match the query."""
        where_clause, params = self._make_where_clause(
            components, variable_name, time_series_type, **features
        )
        cols = "*" if columns is None else ",".join(columns)
        query = f"SELECT {cols} FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        return rows

    def remove(
        self,
        *owners: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Optional[str] = None,
        connection: sqlite3.Connection | None = None,
        **features,
    ) -> list[TimeSeriesMetadata]:
        """Remove all matching rows and return the metadata."""
        con = connection or self._con
        cur = con.cursor()
        where_clause, params = self._make_where_clause(
            owners, variable_name, time_series_type, **features
        )

        query = (
            f"SELECT metadata_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE ({where_clause})"
        )
        rows = execute(cur, query, params=params).fetchall()
        matches = len(rows)
        if not matches:
            msg = "No metadata matching the inputs is stored"
            raise ISNotStored(msg)

        query = f"DELETE FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE ({where_clause})"
        execute(cur, query, params=params)
        if connection is None:
            con.commit()
        count_deleted = execute(cur, "SELECT changes()").fetchall()[0][0]
        if matches != count_deleted:
            msg = f"Bug: Unexpected length mismatch: {matches=} {count_deleted=}"
            raise Exception(msg)

        unique_metadata_uuids = {UUID(row[0]) for row in rows}
        result: list[TimeSeriesMetadata] = []
        for metadata_uuid in unique_metadata_uuids:
            query_count = (
                f"SELECT COUNT(*) FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE metadata_uuid = ?"
            )
            count_association = execute(cur, query_count, params=[str(metadata_uuid)]).fetchone()[
                0
            ]
            if count_association == 0:
                result.append(self._cache_metadata.pop(metadata_uuid))
            else:
                result.append(self._cache_metadata[metadata_uuid])
        return result

    def sql(self, query: str, params: Sequence[str] = ()) -> list[tuple]:
        """Run a SQL query on the time series metadata table."""
        cur = self._con.cursor()
        return execute(cur, query, params=params).fetchall()

    def _insert_rows(self, rows: list[tuple], cur: sqlite3.Cursor) -> None:
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {TIME_SERIES_ASSOCIATIONS_TABLE} VALUES({placeholder})"
        cur.executemany(query, rows)

    def _make_components_str(
        self, params: list[str], *owners: Component | SupplementalAttribute
    ) -> str:
        if not owners:
            msg = "At least one component must be passed."
            raise ISOperationNotAllowed(msg)

        or_clause = "OR ".join((itertools.repeat("owner_uuid = ? ", len(owners))))

        for owner in owners:
            params.append(str(owner.uuid))

        return f"({or_clause})"

    def _make_where_clause(
        self,
        owners: tuple[Component | SupplementalAttribute, ...],
        variable_name: Optional[str],
        time_series_type: Optional[str],
        attribute_hash: Optional[str] = None,
        **features: str,
    ) -> tuple[str, list[str]]:
        params: list[str] = []
        component_str = self._make_components_str(params, *owners)

        if variable_name is None:
            var_str = ""
        else:
            var_str = "AND name = ?"
            params.append(variable_name)

        if time_series_type is None:
            ts_str = ""
        else:
            ts_str = "AND time_series_type = ?"
            params.append(time_series_type)

        if attribute_hash is None and features:
            ua_hash_filter = _make_user_attribute_filter(features, params)
            ua_str = f"AND {ua_hash_filter}"
        else:
            ua_str = ""

        if attribute_hash:
            ua_hash_filter = _make_user_attribute_hash_filter(attribute_hash, params)
            ua_hash = f"AND {ua_hash_filter}"
        else:
            ua_hash = ""

        return f"({component_str} {var_str} {ts_str}) {ua_str} {ua_hash}", params

    def _try_time_series_metadata_by_full_params(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str,
        time_series_type: str,
        column: str,
        **features: str,
    ) -> list[tuple] | None:
        assert variable_name is not None
        assert time_series_type is not None
        where_clause, params = self._make_where_clause(
            (owner,),
            variable_name,
            time_series_type,
            **features,
        )
        # Use the denormalized view
        query = f"SELECT {column} FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        if not rows:
            return None

        return rows

    def _try_get_time_series_metadata_by_full_params(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str,
        time_series_type: str,
        **features: str,
    ) -> TimeSeriesMetadata | None:
        """Attempt to get the metadata by using all parameters.

        This will return the metadata if the user passes all user attributes that exist in the
        time series metadata. This is highly advantageous in cases where one component has a large
        number of time series and each metadata has user attributes. Otherwise, SQLite has to
        parse the JSON values.
        """
        rows = self._try_time_series_metadata_by_full_params(
            owner,
            variable_name,
            time_series_type,
            "metadata",
            **features,
        )
        if rows is None:
            return rows

        if len(rows) > 1:
            msg = f"Found more than one metadata matching inputs: {len(rows)}"
            raise ISOperationNotAllowed(msg)

        return _deserialize_time_series_metadata(rows[0][0])

    def _try_has_time_series_metadata_by_full_params(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str,
        time_series_type: str,
        **features: str,
    ) -> bool:
        """Attempt to check if the metadata is stored by using all parameters. Refer to
        _try_get_time_series_metadata_by_full_params for more information.
        """
        text = self._try_time_series_metadata_by_full_params(
            owner,
            variable_name,
            time_series_type,
            "id",
            **features,
        )
        return text is not None

    def unique_uuids_by_type(self, time_series_type: str):
        query = f"SELECT DISTINCT time_series_uuid from {TIME_SERIES_ASSOCIATIONS_TABLE} where time_series_type = ?"
        params = (time_series_type,)
        uuid_strings = self.sql(query, params)
        return [UUID(ustr[0]) for ustr in uuid_strings]

    def serialize(self, filename: Path | str) -> None:
        with sqlite3.connect(filename) as dst_con:
            schema = [
                "id INTEGER PRIMARY KEY",
                "metadata_uuid TEXT NOT NULL",
                "metadata JSON TEXT NOT NULL",
            ]
            schema_text = ",".join(schema)
            cur = dst_con.cursor()
            execute(cur, f"CREATE TABLE {TIME_SERIES_METADATA_TABLE}({schema_text})")
            query = f"INSERT INTO {TIME_SERIES_METADATA_TABLE} VALUES (?, ?, jsonb(?))"
            metadata = [
                (None, str(metadata_uuid), json.dumps(serialize_value(metadata)))
                for metadata_uuid, metadata in self._cache_metadata.items()
            ]
            cur.executemany(query, metadata)
            dst_con.commit()
        return


@dataclass
class TimeSeriesCounts:
    """Summarizes the counts of time series by component type."""

    time_series_count: int
    # Keys are component_type, time_series_type, initial_time, resolution
    time_series_type_count: dict[tuple[str, str, str, str], int]


def _make_user_attribute_filter(features: dict[str, Any], params: list[str]) -> str:
    attrs = _make_user_attribute_dict(features)
    items = []
    for key, val in attrs.items():
        items.append(f"features->>'$.{key}' = ? ")
        params.append(val)
    return "AND ".join(items)


def _make_user_attribute_hash_filter(attribute_hash: str, params: list[str]) -> str:
    params.append(attribute_hash)
    return "features_hash = ?"


def _make_user_attribute_dict(features: dict[str, Any]) -> dict[str, Any]:
    return {k: features[k] for k in sorted(features)}


def _compute_user_attribute_hash(features: dict[str, Any]) -> str | None:
    if not features:
        return None

    attrs = _make_user_attribute_dict(features)
    return _compute_hash(bytes(json.dumps(attrs), encoding="utf-8"))


def _compute_hash(text: bytes) -> str:
    hash_obj = hashlib.sha256()
    hash_obj.update(text)
    return hash_obj.hexdigest()


def _deserialize_time_series_metadata(text: str) -> TimeSeriesMetadata:
    data = json.loads(text)
    type_metadata = SerializedTypeMetadata(**data.pop(TYPE_METADATA))
    metadata = deserialize_value(data, type_metadata.fields)
    return metadata
