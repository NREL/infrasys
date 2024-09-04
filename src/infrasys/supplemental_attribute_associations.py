"""Stores supplemental attribute associations in SQLite database"""

import hashlib
import itertools
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional, Sequence
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
from infrasys.supplemental_attribute_manager import SupplementalAttribute
from infrasys.time_series_metadata_store import _does_sqlite_support_json
from infrasys.utils.sqlite import execute

class SupplementalAttributeAssociations:
    """Stores supplemental attribute associations in a SQLite database."""

    TABLE_NAME = "supplemental_attribute_associations"

    def __init__(self, con: sqlite3.Connection, initialize: bool = True):
        self._con = con
        if initialize:
            self._create_association_table()
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
    def _create_association_table(self):
        schema = [
            "attribute_uuid TEXT",
            "attribute_type TEXT",
            "component_uuid TEXT",
            "component_type TEXT",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {self.TABLE_NAME}({schema_text})")
        self._create_indexes(cur)
        self._con.commit()
        logger.debug("Created in-memory time series metadata table")

    def _create_indexes(self, cur) -> None:
        execute(
            cur,
            f"CREATE INDEX by_attribute_and_component ON {self.TABLE_NAME} "
            f"(attribute_uuid, component_uuid)",
        )
        execute(cur, f"CREATE INDEX by_component ON {self.TABLE_NAME} (component_uuid)")

    def add(self, component: Component, attribute: SupplementalAttribute) -> None:
        """Add association to the database.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series metadata already stored.
        """

        #attribute_hash = _compute_user_attribute_hash(metadata.user_attributes)
        #where_clause, params = self._make_where_clause(
        #    components,
        #    metadata.variable_name,
        #    metadata.type,
        #    attribute_hash=attribute_hash,
        #    **metadata.user_attributes,
        #)
        #cur = self._con.cursor()

        #query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {where_clause}"
        #res = execute(cur, query, params=params).fetchone()
        #if res[0] > 0:
        #    msg = f"Time series with {metadata=} is already stored."
        #    raise ISAlreadyAttached(msg)

        #Check this later lol
        rows = (
                str(attribute.uuid),
                str(type(attribute)),
                str(component.uuid),
                str(type(component)),
            )

        self._insert_rows(rows)
    
    def _insert_rows(self, rows: list[tuple]) -> None:
        cur = self._con.cursor()
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {self.TABLE_NAME} VALUES({placeholder})"
        try:
            cur.executemany(query, rows)
        finally:
            self._con.commit()