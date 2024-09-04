"""Manages supplemental"""

from collections import defaultdict
import itertools
import sqlite3
from typing import Any, Callable, Iterable, Type
from uuid import UUID
from loguru import logger
from pydantic import Field
from typing_extensions import Annotated

from infrasys.component import Component
from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.models import InfraSysBaseModelWithIdentifers, make_label, get_class_and_name_from_label
from infrasys.supplemental_attribute_associations import SupplementalAttributeAssociations

class SupplementalAttribute(InfraSysBaseModelWithIdentifers):
    name = Annotated[str, Field(frozen=True)]


class SupplementalAttributeManager:
    """Manages supplemental attributes"""

    def __init__(
        self,
        con: sqlite3.Connection, 
        initialize: bool = True,
        **kwargs
    ) -> None:
        self._data: dict[Type, dict[UUID, SupplementalAttribute]] = {}
        self._associations = SupplementalAttributeAssociations(con, initialize)

    def add(self, component: Component, attribute: SupplementalAttribute, allow_existing_time_series: bool = False) -> None:
        """Add one or more supplemental attributes to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        self.raise_if_attached(attribute)
        
        # check later and see if it is necessary
        #if ~allow_existing_time_series and has_time_series(attribute):
        #    msg = f"cannot add an attribute with time_series: {attribute.label}"
        #    raise ISOperationNotAllowed(msg)

        T = type(attribute)
        self._data[T][attribute.uuid] = attribute
        
        self.associations.add_association(component, attribute)

    def remove(self, attribute: SupplementalAttribute) -> Any:
        """Remove the component from the system and return it.

        Notes
        -----
        Users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        self.raise_if_not_attached(attribute)
        #remove association first
        #check if there are any more associations

        #Julia code:
        #T = typeof(supplemental_attribute)
        #pop!(mgr._data[T], get_uuid(supplemental_attribute))
        #prepare_for_removal!(supplemental_attribute)

        return

    def iter(self):

        return

    def raise_if_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is attached to a system."""

        T = type(attribute)
        if ~(T in self._data):
            return 
        
        if attribute.uuid in self._data[T]:
            msg = f"{attribute.label} is already attached to the system"
            raise ISAlreadyAttached(msg)
        
    def raise_if_not_attached(self, attribute: SupplementalAttribute):
        """Raise an exception if this attribute is not attached to a system."""

        T = type(attribute)
        if ~(T in self._data):
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)
        
        if attribute.uuid not in self._data[T]:
            msg = f"{attribute.label} is not attached to the system"
            raise ISNotStored(msg)
