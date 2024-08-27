import sqlite3
from typing import Optional, Type
from uuid import UUID

from loguru import logger

from infrasys.component import Component
from infrasys.utils.sqlite import execute


class ComponentAssociations:
    """Stores associations between components. Allows callers to quickly find components composed
    by other components, such as the generator to which a bus is connected."""

    TABLE_NAME = "component_associations"

    def __init__(self) -> None:
        # This uses a different database because it is not persisted when the system
        # is saved to files. It will be rebuilt during de-serialization.
        self._con = sqlite3.connect(":memory:")
        self._create_metadata_table()

    def _create_metadata_table(self):
        schema = [
            "id INTEGER PRIMARY KEY",
            "component_uuid TEXT",
            "component_type TEXT",
            "attached_component_uuid TEXT",
            "attached_component_type TEXT",
        ]
        schema_text = ",".join(schema)
        cur = self._con.cursor()
        execute(cur, f"CREATE TABLE {self.TABLE_NAME}({schema_text})")
        execute(
            cur,
            f"CREATE INDEX by_c_uuid ON {self.TABLE_NAME}(component_uuid, attached_component_uuid)",
        )
        self._con.commit()
        logger.debug("Created in-memory component associations table")

    def add(self, *components: Component):
        """Store an association between each component and directly attached subcomponents.

        - Inspects the type of each field of each component's type. Looks for subtypes of
          Component and lists of subtypes of Component.
        - Does not consider component fields that are dictionaries or other data structures.
        """
        rows = []
        for component in components:
            for field in type(component).model_fields:
                val = getattr(component, field)
                if isinstance(val, Component):
                    rows.append(self._make_row(component, val))
                elif isinstance(val, list) and val and isinstance(val[0], Component):
                    for item in val:
                        rows.append(self._make_row(component, item))

        if rows:
            self._insert_rows(rows)

    def clear(self) -> None:
        """Clear all component associations."""
        execute(self._con.cursor(), f"DELETE FROM {self.TABLE_NAME}")
        logger.info("Cleared all component associations.")

    def list_child_components(
        self, component: Component, component_type: Optional[Type[Component]] = None
    ) -> list[UUID]:
        """Return a list of all component UUIDS that this component composes.
        For example, return the bus attached to a generator.
        """
        where_clause = "WHERE component_uuid = ?"
        if component_type is None:
            params = [str(component.uuid)]
        else:
            params = [str(component.uuid), component_type.__name__]
            where_clause += " AND attached_component_type = ?"
        query = f"SELECT attached_component_uuid FROM {self.TABLE_NAME} {where_clause}"
        cur = self._con.cursor()
        return [UUID(x[0]) for x in execute(cur, query, params)]

    def list_parent_components(
        self, component: Component, component_type: Optional[Type[Component]] = None
    ) -> list[UUID]:
        """Return a list of all component UUIDS that compose this component.
        For example, return all components connected to a bus.
        """
        where_clause = "WHERE attached_component_uuid = ?"
        if component_type is None:
            params = [str(component.uuid)]
        else:
            params = [str(component.uuid), component_type.__name__]
            where_clause += " AND component_type = ?"
        query = f"SELECT component_uuid FROM {self.TABLE_NAME} {where_clause}"
        cur = self._con.cursor()
        return [UUID(x[0]) for x in execute(cur, query, params)]

    def remove(self, component: Component) -> None:
        """Delete all rows with this component."""
        query = f"""
            DELETE
            FROM {self.TABLE_NAME}
            WHERE component_uuid = ? OR attached_component_uuid = ?
        """
        params = [str(component.uuid), str(component.uuid)]
        execute(self._con.cursor(), query, params)
        logger.debug("Removed all associations with component {}", component.label)

    def _insert_rows(self, rows: list[tuple]) -> None:
        cur = self._con.cursor()
        placeholder = ",".join(["?"] * len(rows[0]))
        query = f"INSERT INTO {self.TABLE_NAME} VALUES({placeholder})"
        try:
            cur.executemany(query, rows)
        finally:
            self._con.commit()

    @staticmethod
    def _make_row(component: Component, attached_component: Component):
        return (
            None,
            str(component.uuid),
            type(component).__name__,
            str(attached_component.uuid),
            type(attached_component).__name__,
        )
