"""Stores supplemental attribute associations in SQLite database"""

import sqlite3
import itertools
import abc
import json
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
    _do_attributes_match,
)
from infrasys.serialization import (
    deserialize_value,
    SerializedTypeMetadata,
)
from infrasys.utils.sqlite import execute
from typing import Any, Optional
from infrasys.exceptions import ISAlreadyAttached, ISOperationNotAllowed, ISNotStored


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
            f"CREATE INDEX by_cu_ct_sau_hash ON {self.TABLE_NAME} "
            f"(component_uuid, component_type, attribute_type, user_attributes_hash)",
        )
        execute(cur, f"CREATE INDEX by_component ON {self.TABLE_NAME} (attribute_uuid)")

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
                str(type(association.attribute)),
                str(association.component.uuid),
                str(type(association.component)),
                attribute_hash,
            )
        ]

        self._insert_rows(rows)

    def get_association(
        self,
        component: Component,
        attribute: SupplementalAttribute,
        # variable_name: Optional[str] = None,
        # time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> SupplementalAttributeAssociations:
        """Return the associations matching the inputs.

        Raises
        ------
        ISOperationNotAllowed
            Raised if more than one metadata instance matches the inputs.
        """

        association_list = self.list_association(
            component,
            attribute,
            # variable_name=variable_name,
            # time_series_type=time_series_type,
            **user_attributes,
        )
        if not association_list:
            msg = "No supplemental attribute matching the inputs is stored"
            raise ISNotStored(msg)

        if len(association_list) > 1:
            msg = f"Found more than association matching inputs: {len(association_list)}"
            raise ISOperationNotAllowed(msg)

        return association_list[0]

    def list_association(
        self,
        component: Component,
        attribute: SupplementalAttribute,
        # variable_name: Optional[str] = None,
        # time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> list[SupplementalAttributeAssociations]:
        """Return a list of associations that match the query."""
        if not self._supports_sqlite_json:
            return [
                x[1]
                for x in self._list_association_no_sql_json(
                    component,
                    attribute
                    # variable_name=variable_name,
                    # time_series_type=time_series_type,
                    **user_attributes,
                )
            ]

        where_clause, params = self._make_where_clause(component, attribute)
        query = f"SELECT metadata FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = execute(cur, query, params=params).fetchall()
        return [_deserialize_association(x[0]) for x in rows]

    def _list_association_no_sql_json(
        self,
        component: Component,
        attribute: SupplementalAttribute,
        # variable_name: Optional[str] = None,
        # time_series_type: Optional[str] = None,
        **user_attributes,
    ) -> list[tuple[int, SupplementalAttributeAssociations]]:
        """Return a list of association that match the query.

        Returns
        -------
        list[tuple[int, TimeSeriesMetadata]]
            The first element of each tuple is the database id field that uniquely identifies the
            row.
        """
        where_clause, params = self._make_where_clause(component, attribute)
        query = f"SELECT id, metadata FROM {self.TABLE_NAME} WHERE {where_clause}"
        cur = self._con.cursor()
        rows = execute(cur, query, params).fetchall()

        association_list = []
        for row in rows:
            association = _deserialize_association(row[1])
            if _do_attributes_match(association.user_attributes, **user_attributes):
                association_list.append((row[0], association))
        return association_list

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
        attribute_str = self._make_attribute_str(params, component)
        print(component_str)
        print(attribute_str)
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

        return f"({component_str}) {ua_str} {ua_hash}", params

    def _make_components_str(self, params: list[str], *components: Component) -> str:
        if not components:
            msg = "At least one component must be passed."
            raise ISOperationNotAllowed(msg)

        or_clause = "OR ".join((itertools.repeat("component_uuid = ? ", len(components))))

        for component in components:
            params.append(str(component.uuid))

        return f"({or_clause})"

    def _make_attribute_str(self, params: list[str], *components: SupplementalAttribute) -> str:
        if not components:
            msg = "At least one component must be passed."
            raise ISOperationNotAllowed(msg)

        or_clause = "OR ".join((itertools.repeat("component_uuid = ? ", len(components))))

        for component in components:
            params.append(str(component.uuid))

        return f"({or_clause})"


def _deserialize_association(text: str) -> SupplementalAttributeAssociations:
    data = json.loads(text)
    # TODO: Check the __association__ type
    type_association = SerializedTypeMetadata(**data.pop("__association__"))
    association = deserialize_value(data, type_association.fields)
    return association
