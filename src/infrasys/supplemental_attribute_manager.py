"""Manages supplemental"""

import sqlite3
from typing import Any, Type
from uuid import UUID

from infrasys.component import Component
from infrasys.exceptions import ISAlreadyAttached, ISNotStored
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.supplemental_attribute_associations import (
    SupplementalAttributeAssociations,
    SupplementalAttributeAssociationsStore,
)


class SupplementalAttributeManager:
    """Manages supplemental attributes"""

    def __init__(self, con: sqlite3.Connection, initialize: bool = True, **kwargs) -> None:
        self._attributes: dict[Type, dict[UUID, SupplementalAttribute]] = {}
        self._associations = SupplementalAttributeAssociationsStore(con, initialize)

    def add(
        self,
        component: Component,
        attribute: SupplementalAttribute,
        allow_existing_time_series: bool = False,
    ) -> None:
        """Add one or more supplemental attributes to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        self.raise_if_attached(attribute)

        # TODO: check later to see if it similar checks are necessary
        # if ~allow_existing_time_series and has_time_series(attribute):
        #    msg = f"cannot add an attribute with time_series: {attribute.label}"
        #    raise ISOperationNotAllowed(msg)
        # if not deserialization_in_progress:
        # TODO: Do we want any checks during deserialization? User could change the JSON.
        # We could prevent the user from changing the JSON with a checksum.
        #    self._check_component_addition(component)
        #    component.check_component_addition()

        T = type(attribute)

        # Add type key if not already defined in dictionary
        if T not in self._attributes:
            self._attributes[T] = {}

        if attribute.uuid not in self._attributes[T]:
            self._attributes[T][attribute.uuid] = attribute
        else:
            msg = f"{attribute.name} with UUID={attribute.uuid} is already stored"
            raise ISAlreadyAttached(msg)

        association = SupplementalAttributeAssociations(component=component, attribute=attribute)

        self._associations.add(association)

    def remove(self, attribute: SupplementalAttribute) -> Any:
        """Remove the component from the system and return it.

        Notes
        -----
        Users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        self.raise_if_not_attached(attribute)
        # remove association first
        # check if there are any more associations

        # Julia code:
        # T = typeof(supplemental_attribute)
        # pop!(mgr._attributes[T], get_uuid(supplemental_attribute))
        # prepare_for_removal!(supplemental_attribute)

        return

    def iter(self):
        return

    def raise_if_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is attached to a system."""

        T = type(attribute)
        if ~(T in self._attributes):
            return

        if attribute.uuid in self._attributes[T]:
            msg = f"{attribute.label} is already attached to the system"
            raise ISAlreadyAttached(msg)

    def raise_if_not_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is not attached to a system."""

        T = type(attribute)
        if ~(T in self._attributes):
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)

        if attribute.uuid not in self._attributes[T]:
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)
