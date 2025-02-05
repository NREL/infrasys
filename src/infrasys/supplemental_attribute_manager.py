"""Manages supplemental"""

import sqlite3
from typing import Any, Callable, Generator, Iterable, Optional, Type, TypeVar
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
        component: Optional[Component],
        attribute: SupplementalAttribute,
        deserialization_in_progress=False,
    ) -> None:
        """Add one or more supplemental attributes to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        if component is None and not deserialization_in_progress:
            msg = "component can only be None when deserialization_in_progress"
            raise Exception(msg)

        already_attached = self.has_attribute(attribute)
        if not deserialization_in_progress and not already_attached:
            attribute.check_supplemental_attribute_addition()

        if not already_attached:
            attr_type = type(attribute)
            if attr_type not in self._attributes:
                self._attributes[attr_type] = {}

            self._attributes[attr_type][attribute.uuid] = attribute

        if component is not None:
            self._associations.add(component, attribute)

    def get_attribute_counts_by_type(self) -> list[dict[str, Any]]:
        """Return a list of dicts of stored attribute counts by type."""
        return self._associations.get_attribute_counts_by_type()

    def get_num_attributes(self) -> int:
        """Return the number of supplemental attributes."""
        return self._associations.get_num_attributes()

    def get_num_components_with_attributes(self) -> int:
        """Return the number of components with supplemental attributes."""
        return self._associations.get_num_components_with_attributes()

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
        attribute_type: Optional[Type[T]] = None,
        filter_func: Optional[Callable[[T], bool]] = None,
    ) -> list[T]:
        type_as_str = None if attribute_type is None else attribute_type.__name__
        uuids = self._associations.list_associated_supplemental_attribute_uuids(
            component, attribute_type=type_as_str
        )
        attrs = []
        for uuid in uuids:
            attr = self.get_by_uuid(uuid)
            if filter_func is None or filter_func(attr):  # type: ignore
                attrs.append(attr)
        return attrs  # type: ignore

    def has_attribute(self, attribute: SupplementalAttribute) -> bool:
        if type(attribute) not in self._attributes:
            return False
        return attribute.uuid in self._attributes[type(attribute)]

    def has_association(self, component: Component, attribute: SupplementalAttribute) -> bool:
        """Return True if the component and supplemental attribute have an association."""
        return self._associations.has_association_by_component_and_attribute(component, attribute)

    def has_association_by_type(
        self,
        component: Component,
        attribute_type: Optional[Type[SupplementalAttribute]] = None,
    ) -> bool:
        """Return true if the component has an association with a supplemental attribute,
        optionally with the given type.
        """
        if attribute_type is None:
            return self._associations.has_association_by_component(component)
        return self._associations.has_association_by_component_and_attribute_type(
            component, attribute_type.__name__
        )

    def remove(
        self, attribute: SupplementalAttribute, association_must_exist: bool = True
    ) -> None:
        """Remove the supplemental attribute from the system.

        Notes
        -----
        Users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        self.raise_if_not_attached(attribute)
        self._associations.remove_association_by_attribute(
            attribute, must_exist=association_must_exist
        )
        attr_type = type(attribute)
        self._attributes[attr_type].pop(attribute.uuid)
        if not self._attributes[attr_type]:
            self._attributes.pop(attr_type)
        logger.debug("Removed supplemental attribute {attribute.label}")

    def remove_attribute_from_component(
        self, component: Component, attribute: SupplementalAttribute
    ) -> None:
        """Remove the supplemental attribute from the component. If the attribute is not attached
        to any other components, remove it from the system.

        Notes
        -----
        Users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        self.raise_if_not_attached(attribute)
        self._associations.remove_association(component, attribute)
        if not self._associations.has_association_by_attribute(attribute):
            self.remove(attribute, association_must_exist=False)

    def iter(
        self,
        *attribute_types: Type[T],
        filter_func: Optional[Callable[[T], bool]] = None,
    ) -> Generator[SupplementalAttribute, None, None]:
        for attr_type in attribute_types:
            yield from self._iter(attr_type, filter_func)

    def iter_all(self) -> Iterable[Any]:
        """Return an iterator over all components."""
        for attr_dict in self._attributes.values():
            yield from attr_dict.values()

    def _iter(
        self,
        attr_type: Type[T],
        filter_func: Optional[Callable[[T], bool]] = None,
    ) -> Generator[Any, None, None]:
        subclasses = attr_type.__subclasses__()
        if subclasses:
            for subclass in subclasses:
                # Recurse.
                yield from self._iter(subclass, filter_func=filter_func)

        if attr_type in self._attributes:
            for val in self._attributes[attr_type].values():
                if filter_func is None or filter_func(val):  # type: ignore
                    yield val

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
