"""Stores supplemental attribute associations in SQLite database"""

import sqlite3
import itertools
import abc
from loguru import logger

from infrasys import Component
from infrasys.models import InfraSysBaseModel
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.time_series_metadata_store import (
    _does_sqlite_support_json,
    _compute_user_attribute_hash,
    _raise_if_unsupported_sql_operation,
    _make_user_attribute_filter,
    _make_user_attribute_hash_filter,
)
from infrasys.utils.sqlite import execute
from typing import Any, Optional
from infrasys.exceptions import ISAlreadyAttached, ISOperationNotAllowed


class SupplementalAttributeAssociations(InfraSysBaseModel, abc.ABC):
    """Defines associations between system components and supplemental attributes"""

    attribute: SupplementalAttribute
    component: Component
    user_attributes: dict[str, Any] = {}

    # @property
    # def label(self) -> str:
    #    """Return the variable_name of the time series array with its type."""
    #    return f"{self.type}.{self.variable_name}"


class SupplementalAttributeAssociationsStore:
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
            "id INTEGER PRIMARY KEY",
            "attribute_uuid TEXT",
            "attribute_type TEXT",
            "component_uuid TEXT",
            "component_type TEXT",
            "user_attributes_hash TEXT",
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

    def add(self, association: SupplementalAttributeAssociations) -> None:
        """Add association to the database.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series metadata already stored.
        """

        attribute_hash = _compute_user_attribute_hash(association.user_attributes)
        where_clause, params = self._make_where_clause(
            association.component,
            association.attribute,
            **association.user_attributes,
        )
        cur = self._con.cursor()

        query = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE {where_clause}"
        res = execute(cur, query, params=params).fetchone()
        if res[0] > 0:
            msg = f"attributes with {association=} is already stored."
            raise ISAlreadyAttached(msg)

        # Check this later
        print(association.component.uuid)
        rows = [
            (
                None,
                str(association.attribute.uuid),
                type(association.attribute),
                str(association.component.uuid),
                type(association.component),
                attribute_hash,
            )
        ]

        self._insert_rows(rows)

    def _insert_rows(self, rows: list[tuple]) -> None:
        cur = self._con.cursor()
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {self.TABLE_NAME} VALUES({placeholder})"
        try:
            cur.executemany(query, rows)
        finally:
            self._con.commit()

    def _make_where_clause(
        self,
        component: Component,
        attribute: SupplementalAttribute,
        attribute_hash: Optional[str] = None,
        **user_attributes: str,
    ) -> tuple[str, list[str]]:
        params: list[str] = []
        component_str = self._make_components_str(params, component)
        attribute_str = self._make_components_str(params, attribute)

        if attribute_hash is None and user_attributes:
            _raise_if_unsupported_sql_operation()
            ua_hash_filter = _make_user_attribute_filter(user_attributes, params)
            ua_str = f"AND {ua_hash_filter}"
        else:
            ua_str = ""

        if attribute_hash:
            ua_hash_filter = _make_user_attribute_hash_filter(attribute_hash, params)
            ua_hash = f"AND {ua_hash_filter}"
        else:
            ua_hash = ""

        return f"({component_str} {attribute_str}) {ua_str} {ua_hash}", params

    def _make_components_str(
        self, params: list[str], *components: Component | SupplementalAttribute
    ) -> str:
        if not components:
            msg = "At least one component must be passed."
            raise ISOperationNotAllowed(msg)

        or_clause = "OR ".join((itertools.repeat("component_uuid = ? ", len(components))))

        for component in components:
            params.append(str(component.uuid))

        return f"({or_clause})"
