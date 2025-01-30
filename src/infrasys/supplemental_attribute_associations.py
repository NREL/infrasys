"""Stores supplemental attribute associations in SQLite database"""

import sqlite3
import itertools
from uuid import UUID

from loguru import logger

from infrasys import Component
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.utils.sqlite import execute
from typing import Any, Optional, Sequence
from infrasys.exceptions import ISAlreadyAttached


class SupplementalAttributeAssociationsStore:
    """Stores supplemental attribute associations in a SQLite database."""

    TABLE_NAME = "supplemental_attribute_associations"

    def __init__(self, con: sqlite3.Connection, initialize: bool = True):
        self._con = con
        if initialize:
            self._create_association_table()
        self._create_indexes()

    def _create_association_table(self):
        schema = [
            "id INTEGER PRIMARY KEY",
            "attribute_uuid TEXT",
            "attribute_type TEXT",
            "component_uuid TEXT",
            "component_type TEXT",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {self.TABLE_NAME}({schema_text})")
        self._con.commit()
        logger.debug("Created in-memory time series metadata table")

    def _create_indexes(self) -> None:
        cur = self._con.cursor()
        execute(
            cur,
            f"CREATE INDEX IF NOT EXISTS by_attribute ON {self.TABLE_NAME} "
            f"(attribute_uuid, component_uuid, component_type)",
        )
        execute(
            cur,
            f"CREATE INDEX IF NOT EXISTS by_component ON {self.TABLE_NAME} "
            f"(component_uuid, attribute_uuid, attribute_type)",
        )

    def add(self, component: Component, attribute: SupplementalAttribute) -> None:
        """Add association to the database.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series metadata already stored.
        """
        query = f"""
            SELECT id FROM {self.TABLE_NAME}
            WHERE attribute_uuid = ? AND component_uuid = ?
            LIMIT 1
        """
        params = (str(attribute.uuid), str(component.uuid))
        cur = self._con.cursor()
        res = execute(cur, query, params=params).fetchone()
        if res:
            msg = f"An association with {component=} {attribute=} is already stored."
            raise ISAlreadyAttached(msg)

        row = (
            None,
            str(attribute.uuid),
            type(attribute).__name__,
            str(component.uuid),
            type(component).__name__,
        )

        placeholder = ",".join(itertools.repeat("?", len(row)))
        query = f"INSERT INTO {self.TABLE_NAME} VALUES ({placeholder})"
        execute(cur, query, params=row)
        self._con.commit()

    def has_association_by_component_attribute(
        self,
        component: Component,
        attribute: SupplementalAttribute,
    ) -> bool:
        """Return the associations matching the inputs.

        Raises
        ------
        ISOperationNotAllowed
            Raised if more than one metadata instance matches the inputs.
        """
        query = f"""
            SELECT id FROM {self.TABLE_NAME}
            WHERE attribute_uuid = ? AND component_uuid = ?
            LIMIT 1
        """
        params = (str(attribute.uuid), str(component.uuid))
        return self._has_rows(query, params)

    def has_association_by_attribute(self, attribute: SupplementalAttribute) -> bool:
        """Return true if there is at least one association matching the inputs."""
        # Note: Unlike the other has_association methods, this is not covered by an index.
        query = f"SELECT id FROM {self.TABLE_NAME} WHERE attribute_uuid = ?"
        params = (str(attribute.uuid),)
        return self._has_rows(query, params)

    def has_association_by_component(self, component: Component) -> bool:
        """Return true if there is at least one association matching the inputs."""
        query = f"SELECT id FROM {self.TABLE_NAME} WHERE component_uuid = ?"
        params = (str(component.uuid),)
        return self._has_rows(query, params)

    def has_association_by_component_attribute_type(
        self, component: Component, attribute_type: str
    ) -> bool:
        query = f"""
            SELECT attribute_uuid
            FROM {self.TABLE_NAME}
            WHERE component_uuid = ? AND attribute_type = ?
            LIMIT 1
        """
        params = (str(component.uuid), attribute_type)
        return self._has_rows(query, params)

    def _has_rows(self, query: str, params: Sequence[Any]) -> bool:
        cur = self._con.cursor()
        res = execute(cur, query, params=params).fetchone()
        return res[0] > 0

    def list_associated_component_uuids(self, attribute: SupplementalAttribute) -> list[UUID]:
        """Return the component UUIDs associated with the attribute."""
        query = f"""
            SELECT component_uuid
            FROM {self.TABLE_NAME}
            WHERE attribute_uuid = ?
        """
        params = (str(attribute.uuid),)
        cur = self._con.cursor()
        rows = execute(cur, query, params=params)
        return [UUID(x[0]) for x in rows]

    def list_associated_supplemental_attribute_uuids(
        self,
        component: Component,
        attribute_type: Optional[str] = None,
    ) -> list[UUID]:
        """Return the supplemental attribute UUIDs associated with the component and attribute type."""
        # TODO: attribute_type must be concrete
        if attribute_type is None:
            where_clause = "component_uuid = ?"
            params = (str(component.uuid),)
        else:
            where_clause = "attribute_type = ? AND component_uuid = ?"
            params = (attribute_type, str(component.uuid))  # type: ignore
        query = f"""
            SELECT attribute_uuid
            FROM {self.TABLE_NAME}
            WHERE {where_clause}
        """
        cur = self._con.cursor()
        rows = execute(cur, query, params=params)
        return [UUID(x[0]) for x in rows]

    def remove_association_by_attribute(
        self,
        attribute: SupplementalAttribute,
    ) -> None:
        """Remove all associations with the given attribute."""
        where_clause = "WHERE attribute_uuid = ?"
        params = (str(attribute.uuid),)
        num_deleted = self._remove_associations(where_clause, params)
        if num_deleted < 1:
            msg = f"Bug: unexpected number of deletions: {num_deleted}. Should have been >= 1."
            raise Exception(msg)

    def remove_association(
        self,
        component: Component,
        attribute: SupplementalAttribute,
    ) -> None:
        """Remove the association between the attribute and component."""
        where_clause = "WHERE attribute_uuid = ? AND component_uuid = ?"
        params = (str(attribute.uuid), str(component.uuid))
        num_deleted = self._remove_associations(where_clause, params)
        if num_deleted != 1:
            msg = f"Bug: unexpected number of deletions: {num_deleted}. Should have been 1."
            raise Exception(msg)

    def remove_associations(self, attribute_type: str) -> None:
        """Remove all associations of the given type."""
        where_clause = "WHERE attribute_type = ?"
        params = (attribute_type,)
        num_deleted = self._remove_associations(where_clause, params)
        logger.debug("Deleted %s supplemental attribute associations", num_deleted)

    def _remove_associations(self, where_clause: str, params: Sequence[Any]) -> int:
        query = f"DELETE FROM {self.TABLE_NAME} {where_clause}"
        cur = self._con.cursor()
        execute(cur, query, params)
        rows = execute(cur, "SELECT CHANGES() AS changes").fetchall()
        assert len(rows) == 1, rows
        row = rows[0]
        logger.debug("Deleted %s rows from the time series metadata table", row[0])
        self._con.commit()
        return row[0]

    def get_attribute_counts_by_type(self) -> list[dict[str, Any]]:
        """Return a list of OrderedDict of stored attribute counts by type."""
        query = f"""
            SELECT
                attribute_type
                ,count(*) AS count
            FROM {self.TABLE_NAME}
            GROUP BY
                attribute_type
            ORDER BY
                attribute_type
        """
        cur = self._con.cursor()
        rows = execute(cur, query).fetchall()
        return [{"type": x.attribute_type, "county": x.count} for x in rows]

    # def get_attribute_summary_table(self) -> pd.DataFrame:
    #    """Return a DataFrame with the number of supplemental attributes by type for components."""
    #    query = f"""
    #        SELECT
    #            attribute_type
    #            ,component_type
    #            ,count(*) AS count
    #        FROM {self.TABLE_NAME}
    #        GROUP BY
    #            attribute_type
    #            ,component_type
    #        ORDER BY
    #            attribute_type
    #            ,component_type
    #    """
    #    cur = self._con.cursor()
    #    rows = execute(cur, query).fetchall()
    #    breakpoint()
    #    pass
    #    #return DataFrame(_execute(associations, query))

    def get_num_attributes(self) -> int:
        """Return the number of supplemental attributes."""
        query = f"""
            SELECT COUNT(DISTINCT attribute_uuid) AS count
            FROM {self.TABLE_NAME}
        """
        cur = self._con.cursor()
        return execute(cur, query)[0].count

    def get_num_components_with_attributes(self) -> int:
        """Return the number of components with supplemental attributes."""
        query = f"""
            SELECT COUNT(DISTINCT component_uuid) AS count
            FROM {self.TABLE_NAME}
        """
        cur = self._con.cursor()
        return execute(cur, query)[0].count
