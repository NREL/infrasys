"""Manages supplemental"""

import sqlite3
from typing import Any, Generator, Iterable, Optional, Type, TypeVar
from uuid import UUID

from loguru import logger

from infrasys.component import Component
from infrasys.exceptions import ISAlreadyAttached, ISNotStored
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.supplemental_attribute_associations import (
    SupplementalAttributeAssociationsStore,
)

T = TypeVar("T", bound="SupplementalAttribute")


class SupplementalAttributeManager:
    """Manages supplemental attributes"""

    def __init__(self, con: sqlite3.Connection, initialize: bool = True, **kwargs) -> None:
        self._attributes: dict[Type, dict[UUID, SupplementalAttribute]] = {}
        self._associations = SupplementalAttributeAssociationsStore(con, initialize=initialize)

    def add(
        self,
        component: Component,
        attribute: SupplementalAttribute,
    ) -> None:
        """Add one or more supplemental attributes to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        self.raise_if_attached(attribute)
        attr_type = type(attribute)

        # TODO: implement something similar to check_component_addition

        if attr_type not in self._attributes:
            self._attributes[attr_type] = {}

        self._attributes[attr_type][attribute.uuid] = attribute
        self._associations.add(component, attribute)

    def get_by_uuid(self, uuid: UUID) -> SupplementalAttribute:
        """Return the supplemental with the given UUID."""
        for attr_dict in self._attributes.values():
            attr = attr_dict.get(uuid)
            if attr is not None:
                return attr
        msg = f"No supplemental attribute with {uuid=} is stored"
        raise ISNotStored(msg)

    def get_component_uuids_with_attribute(self, attribute: SupplementalAttribute) -> list[UUID]:
        """Return all component UUIDs attached to the given attribute."""
        return self._associations.list_associated_component_uuids(attribute)

    def get_attributes_with_component(
        self,
        component: Component,
        attribute_type: Optional[SupplementalAttribute] = None,
    ) -> list[SupplementalAttribute]:
        type_as_str = None if attribute_type is None else str(attribute_type)
        uuids = self._associations.list_associated_supplemental_attribute_uuids(
            component, attribute_type=type_as_str
        )
        return [self.get_by_uuid(x) for x in uuids]

    def remove(self, attribute: SupplementalAttribute) -> Any:
        """Remove the component from the system and return it.

        Notes
        -----
        Users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        self.raise_if_not_attached(attribute)
        self._associations.remove_association_by_attribute(attribute)
        attr_type = type(attribute)
        self._attributes[attr_type].pop(attribute.uuid)
        if not self._attributes[attr_type]:
            self._attributes.pop(attr_type)
        logger.debug("Removed supplemental attribute {attribute.label}")

    def iter(self, *attribute_types: Type[T]) -> Generator[T, None, None]:
        for attr_type in attribute_types:
            yield from self._iter(attr_type)

    def iter_all(self) -> Iterable[Any]:
        """Return an iterator over all components."""
        for attr_dict in self._attributes.values():
            yield from attr_dict.values()

    def _iter(self, attr_type: Type[T]) -> Generator[Any, None, None]:
        subclasses = attr_type.__subclasses__()
        if subclasses:
            for subclass in subclasses:
                # Recurse.
                yield from self._iter(subclass)

        if attr_type in self._attributes:
            yield from self._attributes[attr_type].values()

    def raise_if_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is attached to a system."""
        attr_type = type(attribute)
        if attr_type not in self._attributes:
            return

        if attribute.uuid in self._attributes[attr_type]:
            msg = f"{attribute.label} is already attached to the system"
            raise ISAlreadyAttached(msg)

    def raise_if_not_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is not attached to a system."""
        attr_type = type(attribute)
        if attr_type not in self._attributes:
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)

        if attribute.uuid not in self._attributes[attr_type]:
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)
