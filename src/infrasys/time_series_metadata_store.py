"""Stores time series metadata in a SQLite database."""

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

from loguru import logger

from infrasys.exceptions import ISAlreadyAttached, ISOperationNotAllowed, ISNotStored
from infrasys import Component
from infrasys.serialization import (
    deserialize_value,
    serialize_value,
    SerializedTypeMetadata,
    TYPE_METADATA,
)
from infrasys.time_series_models import TimeSeriesMetadata


class TimeSeriesMetadataStore:
    """Stores time series metadata in a SQLite database."""

    TABLE_NAME = "time_series_metadata"
    DB_FILENAME = "time_series_metadata.db"

    def __init__(self):
        self._con = sqlite3.connect(":memory:")
        self._create_metadata_table()
        self._supports_sqlite_json = _does_sqlite_support_json()
        if not self._supports_sqlite_json:
            # This is true on Ubuntu 22.04, which is used by GitHub runners as of March 2024.
            # It is non-trivial to upgrade SQLite on those platforms.
            # There is code in this file to preserve behavior with less than optimal performance
            # in some cases. We can remove it when we're confident that users and runners have
            # newer SQLite versions.
            logger.debug(
                "SQLite version {} does not support JSON queries, and so time series queries may "
                "have degraded performance.",
                sqlite3.sqlite_version,
            )

    def _create_metadata_table(self):
        schema = [
            "id INTEGER PRIMARY KEY",
            "time_series_uuid TEXT",
            "time_series_type TEXT",
            "initial_time TEXT",
            "resolution TEXT",
            "variable_name TEXT",
            "component_uuid TEXT",
            "component_type TEXT",
            "user_attributes_hash TEXT",
            "metadata JSON",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        self._execute(cur, f"CREATE TABLE {self.TABLE_NAME}({schema_text})")
        self._create_indexes(cur)
        self._con.commit()
        logger.debug("Created in-memory time series metadata table")

    def _create_indexes(self, cur) -> None:
        # Index strategy:
        # 1. Optimize for these user queries with indexes:
        #    1a. all time series attached to one component
        #    1b. time series for one component + variable_name + type
        #    1c. time series for one component with all user attributes
        # 2. Optimize for checks at system.add_time_series. Use all fields and attribute hash.
        # 3. Optimize for returning all metadata for a time series UUID.
        self._execute(
            cur,
            f"CREATE INDEX by_c_vn_tst_hash ON {self.TABLE_NAME} "
            f"(component_uuid, variable_name, time_series_type, user_attributes_hash)",
        )
        self._execute(cur, f"CREATE INDEX by_ts_uuid ON {self.TABLE_NAME} (time_series_uuid)")

    def add(
        self,
        metadata: TimeSeriesMetadata,
        *components: Component,
    ) -> None:
        """Add metadata to the store.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series metadata already stored.
        """
        attribute_hash = _compute_user_attribute_hash(metadata.user_attributes)
        where_clause = self._make_where_clause(
            components,
            metadata.variable_name,
            metadata.type,
            attribute_hash=attribute_hash,
            **metadata.user_attributes,
        )
        cur = self._con.cursor()

        query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {where_clause}"
        res = self._execute(cur, query).fetchone()
        if res[0] > 0:
            msg = f"Time series with {metadata=} is already stored."
            raise ISAlreadyAttached(msg)

        rows = [
            (
                None,  # auto-assigned by sqlite
                str(metadata.time_series_uuid),
                metadata.type,
                str(metadata.initial_time),
                str(metadata.resolution),
                metadata.variable_name,
                str(component.uuid),
                component.__class__.__name__,
                attribute_hash,
                json.dumps(serialize_value(metadata)),
            )
            for component in components
        ]
        self._insert_rows(rows)

    def backup(self, directory: Path | str) -> None:
        """Backup the database to a file in directory."""
        path = directory if isinstance(directory, Path) else Path(directory)
        filename = path / self.DB_FILENAME
        with sqlite3.connect(filename) as con:
            self._con.backup(con)
        con.close()
        logger.info("Backed up the time series metadata to {}", filename)

    def restore(self, directory: Path | str) -> None:
        """Restore the database from a file to memory."""
        path = directory if isinstance(directory, Path) else Path(directory)
        filename = path / self.DB_FILENAME
        with sqlite3.connect(filename) as con:
            con.backup(self._con)
        con.close()
        logger.info("Restored the time series metadata to memory")

    def get_time_series_counts(self) -> "TimeSeriesCounts":
        """Return summary counts of components and time series."""
        query = f"""
            SELECT
                component_type
                ,time_series_type
                ,initial_time
                ,resolution
                ,count(*) AS count
            FROM {self.TABLE_NAME}
            GROUP BY
                component_type
                ,time_series_type
                ,initial_time
                ,resolution
            ORDER BY
                component_type
                ,time_series_type
                ,initial_time
                ,resolution
        """
        cur = self._con.cursor()
        rows = self._execute(cur, query).fetchall()
        time_series_type_count = {(x[0], x[1], x[2], x[3]): x[4] for x in rows}

        time_series_count = self._execute(
            cur, f"SELECT COUNT(DISTINCT time_series_uuid) from {self.TABLE_NAME}"
        ).fetchall()[0][0]

        return TimeSeriesCounts(
            time_series_count=time_series_count,
            time_series_type_count=time_series_type_count,
        )

    def get_metadata(
        self,
        component: Component,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> TimeSeriesMetadata:
        """Return the metadata matching the inputs.

        Raises
        ------
        ISOperationNotAllowed
            Raised if more than one metadata instance matches the inputs.
        """
        if variable_name is not None and time_series_type is not None:
            metadata = self._try_get_time_series_metadata_by_full_params(
                component, variable_name, time_series_type, **user_attributes
            )
            if metadata is not None:
                return metadata

        metadata_list = self.list_metadata(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **user_attributes,
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
        query = (
            f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE time_series_uuid = '{time_series_uuid}'"
        )
        row = self._execute(cur, query).fetchone()
        return row[0] > 0

    def has_time_series_metadata(
        self,
        component: Component,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **user_attributes: Any,
    ) -> bool:
        """Return True if there is time series metadata matching the inputs."""
        if (
            variable_name is not None
            and time_series_type is not None
            and self._try_has_time_series_metadata_by_full_params(
                component, variable_name, time_series_type, **user_attributes
            )
        ):
            return True

        if not self._supports_sqlite_json:
            return bool(
                self._list_metadata_no_sql_json(
                    component,
                    variable_name=variable_name,
                    time_series_type=time_series_type,
                    **user_attributes,
                )
            )

        where_clause = self._make_where_clause(
            (component,), variable_name, time_series_type, **user_attributes
        )
        query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        res = self._execute(cur, query).fetchone()
        return res[0] > 0

    def list_existing_time_series(self, time_series_uuids: list[UUID]) -> set[UUID]:
        """Return the UUIDs that are present."""
        cur = self._con.cursor()
        uuids = ",".join([f"'{x}'" for x in time_series_uuids])
        query = (
            f"SELECT time_series_uuid FROM {self.TABLE_NAME} WHERE time_series_uuid IN ({uuids})"
        )
        rows = self._execute(cur, query).fetchall()
        return {UUID(x[0]) for x in rows}

    def list_missing_time_series(self, time_series_uuids: list[UUID]) -> set[UUID]:
        """Return the UUIDs that are not present."""
        existing_uuids = set(self.list_existing_time_series(time_series_uuids))
        return set(time_series_uuids) - existing_uuids

    def list_metadata(
        self,
        *components: Component,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> list[TimeSeriesMetadata]:
        """Return a list of metadata that match the query."""
        if not self._supports_sqlite_json:
            return [
                x[1]
                for x in self._list_metadata_no_sql_json(
                    *components,
                    variable_name=variable_name,
                    time_series_type=time_series_type,
                    **user_attributes,
                )
            ]

        where_clause = self._make_where_clause(
            components, variable_name, time_series_type, **user_attributes
        )
        query = f"SELECT metadata FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = self._execute(cur, query).fetchall()
        return [_deserialize_time_series_metadata(x[0]) for x in rows]

    def _list_metadata_no_sql_json(
        self,
        *components: Component,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> list[tuple[int, TimeSeriesMetadata]]:
        """Return a list of metadata that match the query.

        Returns
        -------
        list[tuple[int, TimeSeriesMetadata]]
            The first element of each tuple is the database id field that uniquely identifies the
            row.
        """
        where_clause = self._make_where_clause(components, variable_name, time_series_type)
        query = f"SELECT id, metadata FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = self._execute(cur, query).fetchall()

        metadata_list = []
        for row in rows:
            metadata = _deserialize_time_series_metadata(row[1])
            if _do_attributes_match(metadata.user_attributes, **user_attributes):
                metadata_list.append((row[0], metadata))
        return metadata_list

    def list_rows(
        self,
        *components: Component,
        variable_name: Optional[str] = None,
        time_series_type: Optional[str] = None,
        columns=None,
        **user_attributes,
    ) -> list[tuple]:
        """Return a list of rows that match the query."""
        if not self._supports_sqlite_json and user_attributes:
            msg = (
                "list_rows is not supported with user_attributes with SQLite version "
                "{sqlite3.sqlite_version}"
            )
            raise ISOperationNotAllowed(msg)

        where_clause = self._make_where_clause(
            components, variable_name, time_series_type, **user_attributes
        )
        cols = "*" if columns is None else ",".join(columns)
        query = f"SELECT {cols} FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = self._execute(cur, query).fetchall()
        return rows

    def remove(
        self,
        *components: Component,
        variable_name: str | None = None,
        time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> list[UUID]:
        """Remove all matching rows and return the time series UUIDs."""
        cur = self._con.cursor()
        if not self._supports_sqlite_json:
            ts_uuids = set()
            ids = []
            for id_, metadata in self._list_metadata_no_sql_json(
                *components,
                variable_name=variable_name,
                time_series_type=time_series_type,
                **user_attributes,
            ):
                ts_uuids.add(metadata.time_series_uuid)
                ids.append(id_)
            id_str = ",".join([str(x) for x in ids])
            query = f"DELETE FROM {self.TABLE_NAME} WHERE id IN ({id_str})"
            self._execute(cur, query)
            count_deleted = self._execute(cur, "SELECT changes()").fetchall()[0][0]
            if count_deleted != len(ids):
                msg = f"Bug: Unexpected length mismatch {len(ts_uuids)=} {count_deleted=}"
                raise Exception(msg)
            self._con.commit()
            return list(ts_uuids)

        where_clause = self._make_where_clause(
            components, variable_name, time_series_type, **user_attributes
        )
        query = f"SELECT time_series_uuid FROM {self.TABLE_NAME} WHERE {where_clause}"
        uuids = [UUID(x[0]) for x in self._execute(cur, query).fetchall()]

        query = f"DELETE FROM {self.TABLE_NAME} WHERE ({where_clause})"
        self._execute(cur, query)
        self._con.commit()
        count_deleted = self._execute(cur, "SELECT changes()").fetchall()[0][0]
        if len(uuids) != count_deleted:
            msg = f"Bug: Unexpected length mismatch: {len(uuids)=} {count_deleted=}"
            raise Exception(msg)
        return uuids

    def sql(self, query: str) -> list[tuple]:
        """Run a SQL query on the time series metadata table."""
        cur = self._con.cursor()
        return self._execute(cur, query).fetchall()

    def _execute(self, cursor: sqlite3.Cursor, query: str) -> Any:
        logger.trace("SQL query: {}", query)
        return cursor.execute(query)

    def _insert_rows(self, rows: list[tuple]) -> None:
        cur = self._con.cursor()
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {self.TABLE_NAME} VALUES({placeholder})"
        try:
            cur.executemany(query, rows)
        finally:
            self._con.commit()

    def _make_components_str(self, *components: Component) -> str:
        if not components:
            msg = "At least one component must be passed."
            raise ISOperationNotAllowed(msg)
        or_clause = "OR ".join([f"component_uuid = '{x.uuid}'" for x in components])
        return f"({or_clause})"

    def _make_where_clause(
        self,
        components: tuple[Component, ...],
        variable_name: Optional[str],
        time_series_type: Optional[str],
        attribute_hash: Optional[str] = None,
        **user_attributes: str,
    ) -> str:
        component_str = self._make_components_str(*components)
        var_str = "" if variable_name is None else f"AND variable_name = '{variable_name}'"
        ts_str = "" if time_series_type is None else f"AND time_series_type = '{time_series_type}'"
        ua_str = (
            f"AND {_make_user_attribute_filter(user_attributes)}"
            if attribute_hash is None and user_attributes
            else ""
        )
        if ua_str:
            _raise_if_unsupported_sql_operation()

        ua_hash = (
            f"AND {_make_user_attribute_hash_filter(attribute_hash)}" if attribute_hash else ""
        )
        return f"({component_str} {var_str} {ts_str}) {ua_str} {ua_hash}"

    def _try_time_series_metadata_by_full_params(
        self,
        component: Component,
        variable_name: str,
        time_series_type: str,
        column: str,
        **user_attributes: str,
    ) -> list[tuple] | None:
        assert variable_name is not None
        assert time_series_type is not None
        where_clause = self._make_where_clause(
            (component,),
            variable_name,
            time_series_type,
            attribute_hash=_compute_user_attribute_hash(user_attributes),
            **user_attributes,
        )
        query = f"SELECT {column} FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = self._execute(cur, query).fetchall()
        if not rows:
            return None

        return rows

    def _try_get_time_series_metadata_by_full_params(
        self,
        component: Component,
        variable_name: str,
        time_series_type: str,
        **user_attributes: str,
    ) -> TimeSeriesMetadata | None:
        """Attempt to get the metadata by using all parameters.

        This will return the metadata if the user passes all user attributes that exist in the
        time series metadata. This is highly advantageous in cases where one component has a large
        number of time series and each metadata has user attributes. Otherwise, SQLite has to
        parse the JSON values.
        """
        rows = self._try_time_series_metadata_by_full_params(
            component,
            variable_name,
            time_series_type,
            "metadata",
            **user_attributes,
        )
        if rows is None:
            return rows

        if len(rows) > 1:
            msg = f"Found more than one metadata matching inputs: {len(rows)}"
            raise ISOperationNotAllowed(msg)

        return _deserialize_time_series_metadata(rows[0][0])

    def _try_has_time_series_metadata_by_full_params(
        self,
        component: Component,
        variable_name: str,
        time_series_type: str,
        **user_attributes: str,
    ) -> bool:
        """Attempt to check if the metadata is stored by using all parameters. Refer to
        _try_get_time_series_metadata_by_full_params for more information.
        """
        text = self._try_time_series_metadata_by_full_params(
            component,
            variable_name,
            time_series_type,
            "id",
            **user_attributes,
        )
        return text is not None


@dataclass
class TimeSeriesCounts:
    """Summarizes the counts of time series by component type."""

    time_series_count: int
    # Keys are component_type, time_series_type, initial_time, resolution
    time_series_type_count: dict[tuple[str, str, str, str], int]


def _make_user_attribute_filter(user_attributes: dict[str, Any]) -> str:
    attrs = _make_user_attribute_dict(user_attributes)
    text = "AND ".join([f"metadata->>'$.user_attributes.{k}' = '{v}'" for k, v in attrs.items()])
    return f"({text})"


def _make_user_attribute_hash_filter(attribute_hash) -> str:
    return f"user_attributes_hash = '{attribute_hash}'"


def _make_user_attribute_dict(user_attributes: dict[str, Any]) -> dict[str, Any]:
    return {k: user_attributes[k] for k in sorted(user_attributes)}


def _compute_user_attribute_hash(user_attributes: dict[str, Any]) -> str | None:
    if not user_attributes:
        return None

    attrs = _make_user_attribute_dict(user_attributes)
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


def _does_sqlite_support_json() -> bool:
    if "__INFRASYS_NON_JSON_SQLITE__" in os.environ:
        return False

    version = sqlite3.sqlite_version_info
    return not (version[0] == 3 and version[1] < 38)


def _raise_if_unsupported_sql_operation() -> None:
    if not _does_sqlite_support_json():
        msg = (
            "Operations that perform a JSON search with SQLite require version 3.38 or later: "
            f"{sqlite3.sqlite_version}. Please submit a bug report to the infrasys developers."
        )
        raise ISOperationNotAllowed(msg)


def _do_attributes_match(db_attributes: dict[str, Any], **user_attributes: str) -> bool:
    for key, val in user_attributes.items():
        if db_attributes.get(key) != val:
            return False
    return True
