"""Stores time series metadata in a SQLite database."""

import itertools
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from uuid import UUID

import orjson as json
from loguru import logger

from infrasys import (
    KEY_VALUE_STORE_TABLE,
    TIME_SERIES_ASSOCIATIONS_TABLE,
    TS_METADATA_FORMAT_VERSION,
    Component,
)
from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.serialization import (
    SerializedTypeMetadata,
    deserialize_type,
    serialize_value,
)
from infrasys.supplemental_attribute_manager import SupplementalAttribute
from infrasys.time_series_models import (
    NonSequentialTimeSeriesMetadataBase,
    SingleTimeSeriesMetadataBase,
    TimeSeriesMetadata,
)
from infrasys.utils.metadata_utils import create_associations_table
from infrasys.utils.sqlite import backup, execute
from infrasys.utils.time_utils import to_iso_8601


class TimeSeriesMetadataStore:
    """Stores time series metadata in a SQLite database."""

    def __init__(self, con: sqlite3.Connection, initialize: bool = True):
        self._con = con
        if initialize:
            assert create_associations_table(connection=self._con)
            self._create_key_value_store()
            self._create_indexes()
        self._cache_metadata: dict[UUID, TimeSeriesMetadata] = {}

    def _load_metadata_into_memory(self):
        query = f"SELECT * FROM {TIME_SERIES_ASSOCIATIONS_TABLE}"
        cursor = self._con.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in rows]
        for row in rows:
            assert (
                "features" in row
            ), f"Bug: Features missing from {TIME_SERIES_ASSOCIATIONS_TABLE} table."
            metadata = _deserialize_time_series_metadata(row)
            self._cache_metadata[metadata.uuid] = metadata
        return

    def _create_key_value_store(self):
        schema = ["key TEXT PRIMARY KEY", "value JSON NOT NULL"]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {KEY_VALUE_STORE_TABLE}({schema_text})")

        rows = [("version", TS_METADATA_FORMAT_VERSION)]
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {KEY_VALUE_STORE_TABLE}(key, value) VALUES({placeholder})"
        cur.executemany(query, rows)
        self._con.commit()
        logger.debug("Created metadata table")

    def _create_indexes(self) -> None:
        # Index strategy:
        # 1. Optimize for these user queries with indexes:
        #    1a. all time series attached to one component
        #    1b. time series for one component + variable_name + type
        #    1c. time series for one component with all user attributes
        # 2. Optimize for checks at system.add_time_series. Use all fields.
        # 3. Optimize for returning all metadata for a time series UUID.
        cur = self._con.cursor()
        execute(
            cur,
            f"CREATE INDEX by_c_vn_tst_hash ON {TIME_SERIES_ASSOCIATIONS_TABLE} "
            f"(owner_uuid, time_series_type, name, resolution, features)",
        )
        execute(
            cur,
            f"CREATE INDEX by_ts_uuid ON {TIME_SERIES_ASSOCIATIONS_TABLE} (time_series_uuid)",
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
            metadata.name,
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

        # Will probably need to refactor if we introduce more metadata classes.
        if isinstance(metadata, SingleTimeSeriesMetadataBase):
            resolution = to_iso_8601(metadata.resolution)
            initial_time = str(metadata.initial_timestamp)
            horizon = None
            interval = None
            window_count = None
        elif isinstance(metadata, NonSequentialTimeSeriesMetadataBase):
            resolution = None
            initial_time = None
            horizon = None
            interval = None
            window_count = None
        else:
            raise NotImplementedError

        units = None
        if metadata.units:
            units = json.dumps(serialize_value(metadata.units))

        rows = [
            {
                "time_series_uuid": str(metadata.time_series_uuid),
                "time_series_type": metadata.type,
                "initial_timestamp": initial_time,
                "resolution": resolution,
                "horizon": horizon,
                "interval": interval,
                "window_count": window_count,
                "length": metadata.length if hasattr(metadata, "length") else None,
                "name": metadata.name,
                "owner_uuid": str(owner.uuid),
                "owner_type": owner.__class__.__name__,
                "owner_category": "Component",
                "features": make_features_string(metadata.features),
                "units": units,
                "metadata_uuid": str(metadata.uuid),
            }
            for owner in owners
        ]
        self._insert_rows(rows, cur)
        if connection is None:
            con.commit()

        self._cache_metadata[metadata.uuid] = metadata
        # else, commit/rollback will occur at a higer level.
        return

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
        name: Optional[str] = None,
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
            name=name,
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
        uuids = self._get_metadata_uuids_by_filter(
            (owner,), variable_name, time_series_type, **features
        )
        return bool(uuids)

    def list_existing_time_series(self, time_series_uuids: Iterable[UUID]) -> set[UUID]:
        """Return the UUIDs that are present in the database with at least one reference."""
        cur = self._con.cursor()
        params = tuple(str(x) for x in time_series_uuids)
        if not params:
            return set()
        uuids = ",".join(itertools.repeat("?", len(params)))
        query = f"SELECT DISTINCT time_series_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE time_series_uuid IN ({uuids})"
        rows = execute(cur, query, params=params).fetchall()
        return {UUID(x[0]) for x in rows}

    def list_existing_time_series_uuids(self) -> set[UUID]:
        """Return the UUIDs that are present."""
        cur = self._con.cursor()
        query = f"SELECT DISTINCT time_series_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE}"
        rows = execute(cur, query).fetchall()
        return {UUID(x[0]) for x in rows}

    def list_missing_time_series(self, time_series_uuids: Iterable[UUID]) -> set[UUID]:
        """Return the time_series_uuids that are no longer referenced by any owner."""
        existing_uuids = self.list_existing_time_series(time_series_uuids)
        return set(time_series_uuids) - existing_uuids

    def list_metadata(
        self,
        *owners: Component | SupplementalAttribute,
        name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features,
    ) -> list[TimeSeriesMetadata]:
        """Return a list of metadata that match the query."""
        metadata_uuids = self._get_metadata_uuids_by_filter(
            owners, name, time_series_type, **features
        )
        return [
            self._cache_metadata[uuid] for uuid in metadata_uuids if uuid in self._cache_metadata
        ]

    def list_metadata_with_time_series_uuid(
        self,
        time_series_uuid: UUID,
        limit: int | None = None,
        connection: sqlite3.Connection | None = None,
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
        query = f"""
        SELECT
            metadata_uuid
        FROM {TIME_SERIES_ASSOCIATIONS_TABLE}
        WHERE
            time_series_uuid = ? {limit_str}
        """
        con = connection or self._con
        breakpoint()
        cur = con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        return [
            self._cache_metadata[UUID(x[0])] for x in rows if UUID(x[0]) in self._cache_metadata
        ]

    def list_metadata_with_time_series_uuids(
        self,
        time_series_uuids: list[UUID],
        connection: sqlite3.Connection | None = None,
    ) -> list[TimeSeriesMetadata]:
        """Return metadata attached to the given time_series_uuid.

        Parameters
        ----------
        time_series_uuid
            The UUID of the time series.
        limit
            The maximum number of metadata to return. If None, all metadata are returned.
        """
        query = f"""
        SELECT DISTINCT
            metadata_uuid
        FROM {TIME_SERIES_ASSOCIATIONS_TABLE}
        WHERE
            time_series_uuid = ?
        """
        con = connection or self._con
        cur = con.cursor()
        for uuid in time_series_uuids:
            execute(cur, query, params=(str(uuid),))
            for row in cur:
                metadata_uuid_str = row[0]
                metadata_uuid_obj = UUID(metadata_uuid_str)
                if metadata_uuid_obj in self._cache_metadata:
                    yield self._cache_metadata[metadata_uuid_obj]

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
            query_count = f"SELECT COUNT(*) FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE uuid = ?"
            count_association = execute(cur, query_count, params=[str(metadata_uuid)]).fetchone()[
                0
            ]
            if count_association == 0:
                result.append(self._cache_metadata.pop(metadata_uuid))
            else:
                result.append(self._cache_metadata[metadata_uuid])
        return result

    def remove_by_metadata(
        self,
        metadata: TimeSeriesMetadata,
        connection: sqlite3.Connection | None = None,
    ) -> TimeSeriesMetadata:
        """Remove all associations for a given metadata and return the metadata."""
        con = connection or self._con
        cur = con.cursor()

        query = f"DELETE FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE metadata_uuid = ?"
        cur.execute(query, (str(metadata.uuid),))

        if con is None:
            con.commit()

        if metadata.uuid in self._cache_metadata:
            return self._cache_metadata.pop(metadata.uuid)
        else:
            return metadata

    def batch_remove_by_metadata(
        self,
        metadata_list: list[TimeSeriesMetadata],
        connection: sqlite3.Connection | None = None,
        sqlite_max_vars: int = 999,
    ) -> list[TimeSeriesMetadata]:
        """Remove multiple associations for a list of metadata."""
        con = connection or self._con
        cur = con.cursor()
        metadata_uuids = [str(meta.uuid) for meta in metadata_list]
        if not metadata_uuids:  # Should not happen if metadata_list is not empty, but safe check
            return []
        effective_batch_size = max(1, sqlite_max_vars)

        cur = con.cursor()
        try:
            for i in range(0, len(metadata_uuids), effective_batch_size):
                chunk = metadata_uuids[i : i + effective_batch_size]
                chunk_uuid_strings = tuple(meta for meta in chunk)

                if not chunk_uuid_strings:  # Should not happen, but safeguard
                    continue

                placeholders = ",".join("?" * len(chunk_uuid_strings))
                query = f"DELETE FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE metadata_uuid IN ({placeholders})"

                cur.execute(query, chunk_uuid_strings)
        finally:
            cur.close()

        processed_metadata = []
        for metadata_obj in metadata_list:
            uuid_to_remove = metadata_obj.uuid
            if uuid_to_remove in self._cache_metadata:
                processed_metadata.append(self._cache_metadata.pop(uuid_to_remove))
            else:
                processed_metadata.append(metadata_obj)

        return processed_metadata

    def sql(self, query: str, params: Sequence[str] = ()) -> list[tuple]:
        """Run a SQL query on the time series metadata table."""
        cur = self._con.cursor()
        return execute(cur, query, params=params).fetchall()

    def _insert_rows(self, rows: list[dict], cur: sqlite3.Cursor) -> None:
        query = f"""
        INSERT INTO {TIME_SERIES_ASSOCIATIONS_TABLE} (
            time_series_uuid, time_series_type, initial_timestamp, resolution,
            length, name, owner_uuid, owner_type, owner_category, features, units,
            metadata_uuid
        ) VALUES (
            :time_series_uuid, :time_series_type, :initial_timestamp,
            :resolution, :length, :name, :owner_uuid, :owner_type,
            :owner_category, :features, :units, :metadata_uuid
        )
        """
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

        if features:
            feat_filter = _make_features_filter(features, params)
            feat_str = f"AND {feat_filter}"
        else:
            feat_str = ""

        return f"({component_str} {var_str} {ts_str}) {feat_str}", params

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

    def unique_uuids_by_type(self, time_series_type: str):
        query = f"SELECT DISTINCT time_series_uuid from {TIME_SERIES_ASSOCIATIONS_TABLE} where time_series_type = ?"
        params = (time_series_type,)
        uuid_strings = self.sql(query, params)
        return [UUID(ustr[0]) for ustr in uuid_strings]

    def serialize(self, filename: Path | str) -> None:
        """Serialize SQLite to file."""
        with sqlite3.connect(filename) as dst_con:
            self._con.backup(dst_con)
            cur = dst_con.cursor()
            # Drop all index from the database that were created manually (sql not null)
            index_to_drop = execute(
                cur, "SELECT name FROM sqlite_master WHERE type ='index' AND sql IS NOT NULL"
            ).fetchall()
            for index in index_to_drop:
                execute(cur, f"DROP INDEX {index[0]}")
        dst_con.close()
        backup(self._con, filename)
        return

    def _get_metadata_uuids_by_filter(
        self,
        owners: tuple[Component | SupplementalAttribute, ...],
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features: Any,
    ) -> list[UUID]:
        """Get metadata UUIDs that match the filter criteria using progressive filtering."""
        cur = self._con.cursor()

        where_clause, params = self._make_where_clause(
            owners, variable_name, time_series_type, **features
        )
        query = f"SELECT metadata_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        rows = execute(cur, query, params=params).fetchall()

        if rows or not features:
            return [UUID(row[0]) for row in rows]

        where_clause, params = self._make_where_clause(owners, variable_name, time_series_type)
        features_str = make_features_string(features)
        query = f"SELECT metadata_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause} AND features = ?"
        params.append(features_str)
        rows = execute(cur, query, params=params).fetchall()

        if rows:
            return [UUID(row[0]) for row in rows]

        conditions = []
        like_params = []
        where_clause, base_params = self._make_where_clause(
            owners, variable_name, time_series_type
        )
        like_params.extend(base_params)

        for key, value in features.items():
            conditions.append("features LIKE ?")
            like_params.append(f'%"{key}":"{value}"%')

        if conditions:
            query = f"SELECT metadata_uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause} AND ({' AND '.join(conditions)})"
            rows = execute(cur, query, params=like_params).fetchall()

        return [UUID(row[0]) for row in rows]


@dataclass
class TimeSeriesCounts:
    """Summarizes the counts of time series by component type."""

    time_series_count: int
    # Keys are component_type, time_series_type, initial_time, resolution
    time_series_type_count: dict[tuple[str, str, str, str], int]


def _make_features_filter(features: dict[str, Any], params: list[str]) -> str:
    conditions = []
    for key, value in features.items():
        conditions.append("features LIKE ?")
        params.append(f'%"{key}":"{value}"%')
    return " AND ".join(conditions)


def _make_features_dict(features: dict[str, Any]) -> dict[str, Any]:
    return {k: features[k] for k in sorted(features)}


def _deserialize_time_series_metadata(data: dict) -> TimeSeriesMetadata:
    time_series_type = data.pop("time_series_type")
    serialized_type = SerializedTypeMetadata.validate_python(
        {
            "module": "infrasys",
            "type": time_series_type,
            "serialized_type": "base",
        }
    )
    metadata = deserialize_type(serialized_type).get_time_series_metadata_type()

    # Deserialize JSON columns
    for column in ["features", "scaling_factor_multiplier", "units"]:
        if data.get(column):
            data[column] = json.loads(data[column])

    # Features requires special handling since it is a sorter array with key value pairs.
    if data.get("features"):
        data["features"] = data["features"][0]
    else:
        data["features"] = {}

    data["uuid"] = data.pop("metadata_uuid")
    metadata_instance = metadata.model_validate(
        {key: value for key, value in data.items() if key in metadata.model_fields}
    )
    return metadata_instance


def make_features_string(features: dict[str, Any]) -> str:
    """Serializes a dictionary of features into a sorted string."""
    data = [{key: value} for key, value in sorted(features.items())]
    return json.dumps(data).decode()
