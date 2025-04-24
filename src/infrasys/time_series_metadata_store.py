"""Stores time series metadata in a SQLite database."""

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
    TS_METADATA_FORMAT_VERSION,
    Component,
)
from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.serialization import (
    TYPE_METADATA,
    SerializedBaseType,
    SerializedTypeMetadata,
    deserialize_value,
)
from infrasys.supplemental_attribute_manager import SupplementalAttribute
from infrasys.time_series_models import (
    NonSequentialTimeSeriesMetadataBase,
    SingleTimeSeriesMetadataBase,
    TimeSeriesMetadata,
)
from infrasys.utils.metadata_utils import create_associations_table
from infrasys.utils.sqlite import execute
from infrasys.utils.time_utils import to_iso_8601


class TimeSeriesMetadataStore:
    """Stores time series metadata in a SQLite database."""

    def __init__(self, con: sqlite3.Connection, initialize: bool = True):
        self._con = con
        if initialize:
            assert create_associations_table(connection=self._con)
            self._create_key_value_store()
        self._cache_metadata: dict[UUID, TimeSeriesMetadata] = {}

    def _load_metadata_into_memory(self):
        query = f"SELECT * FROM {TIME_SERIES_ASSOCIATIONS_TABLE}"
        cursor = self._con.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in rows]
        for row in rows:
            # Features require special handling due to special indexing that we perform.
            features = json.loads(row.get("features")) or {}
            if features and isinstance(features[0], dict):
                features = features[0]
            row["features"] = features
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
                "uuid": str(metadata.uuid),
                "serialization_info": make_serialization_info(metadata),
            }
            for owner in owners
        ]
        self._insert_rows(rows, cur)
        if connection is None:
            self._con.commit()

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

    def list_missing_time_series(self, time_series_uuids: Iterable[UUID]) -> set[UUID]:
        """Return the time_series_uuids that are no longer referenced by any owner."""
        existing_uuids = self.list_existing_time_series(time_series_uuids)
        return set(time_series_uuids) - existing_uuids

    def list_metadata(
        self,
        *owners: Component | SupplementalAttribute,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **features,
    ) -> list[TimeSeriesMetadata]:
        """Return a list of metadata that match the query."""
        metadata_uuids = self._get_metadata_uuids_by_filter(
            owners, variable_name, time_series_type, **features
        )
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
            uuid
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

        query = f"SELECT uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE ({where_clause})"
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

    def sql(self, query: str, params: Sequence[str] = ()) -> list[tuple]:
        """Run a SQL query on the time series metadata table."""
        cur = self._con.cursor()
        return execute(cur, query, params=params).fetchall()

    def _insert_rows(self, rows: list[dict], cur: sqlite3.Cursor) -> None:
        query = f"""
        INSERT INTO `{TIME_SERIES_ASSOCIATIONS_TABLE}` (
            time_series_uuid, time_series_type, initial_timestamp, resolution,
            length, name, owner_uuid, owner_type, owner_category, features,
            serialization_info, uuid
        ) VALUES (
            :time_series_uuid, :time_series_type, :initial_timestamp,
            :resolution, :length, :name, :owner_uuid, :owner_type,
            :owner_category, :features, :serialization_info, :uuid
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
        with sqlite3.connect(filename) as dst_con:
            dst_con.commit()
        dst_con.close()
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
        query = f"SELECT uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause}"
        rows = execute(cur, query, params=params).fetchall()

        if rows or not features:
            return [UUID(row[0]) for row in rows]

        where_clause, params = self._make_where_clause(owners, variable_name, time_series_type)
        features_str = make_features_string(features)
        query = f"SELECT uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause} AND features = ?"
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
            query = f"SELECT uuid FROM {TIME_SERIES_ASSOCIATIONS_TABLE} WHERE {where_clause} AND ({' AND '.join(conditions)})"
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
    # data = json.loads(text)
    metadata = json.loads(data.pop("serialization_info"))[TYPE_METADATA]
    data.update({k: metadata.pop(k) for k in ["quantity_metadata", "normalization"]})
    validated_metadata = SerializedTypeMetadata.validate_python(metadata)
    metadata = deserialize_value(data, validated_metadata)
    return metadata


def make_features_string(features: dict[str, Any]) -> str:
    """Serializes a dictionary of features into a sorted string."""
    data = [{key: value} for key, value in sorted(features.items())]
    return json.dumps(data, separators=(",", ":"))


def make_serialization_info(metadata: TimeSeriesMetadata) -> str:
    """Serialize information."""
    metadata_type = SerializedTypeMetadata.validate_python(
        SerializedBaseType(
            module=metadata.__module__,
            type=metadata.__class__.__name__,
        )
    ).model_dump()
    metadata_seriarlized = metadata.model_dump(mode="json", round_trip=True)
    serialized_info = {
        TYPE_METADATA: {
            "quantity_metadata": metadata_seriarlized.get("quantity_metadata"),
            "normalization": metadata_seriarlized.get("normalization"),
            **metadata_type,
        }
    }
    return json.dumps(serialized_info, separators=(",", ":"))
