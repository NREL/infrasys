"""Defines base models for components."""

from typing import Any, Optional
from uuid import UUID

from pydantic import Field, field_serializer
from rich import print as _pprint
from typing_extensions import Annotated

from infrasys.base_quantity import BaseQuantity
from infrasys.exceptions import (
    ISNotStored,
    ISAlreadyAttached,
)
from infrasys.models import (
    InfraSysBaseModelWithIdentifers,
)
from infrasys.serialization import (
    SerializedTypeMetadata,
    SerializedComponentReference,
    SerializedQuantityType,
    TYPE_METADATA,
    serialize_value,
)


class Component(InfraSysBaseModelWithIdentifers):
    """Base class for all models representing entities that get attached to a System."""

    name: Annotated[str, Field(frozen=True)]
    system_uuid: Annotated[Optional[UUID], Field(repr=False, exclude=True)] = None

    @field_serializer("system_uuid")
    def _serialize_system_uuid(self, _) -> str:
        return str(self.system_uuid)

    def check_component_addition(self, system_uuid: UUID) -> None:
        """Perform checks on the component before adding it to a system."""

    def is_attached(self, system_uuid: Optional[UUID] = None) -> bool:
        """Return True if the component is attached to a system.

        Parameters
        ----------
        system_uuid : UUID
            Only return True if the component is attached to the system with this UUID.
        """
        if self.system_uuid is None:
            return False
        return self.system_uuid == system_uuid

    def model_dump_custom(self, *args, **kwargs) -> dict[str, Any]:
        """Custom serialization for this package"""
        refs = {x: self._model_dump_field(x) for x in self.model_fields}
        exclude = kwargs.get("exclude", [])
        exclude += list(set(exclude).union(refs))
        kwargs["exclude"] = exclude
        data = serialize_value(self, *args, **kwargs)
        data.update(refs)
        return data

    def _model_dump_field(self, field) -> Any:
        val = getattr(self, field)
        if isinstance(val, Component):
            val = {TYPE_METADATA: serialize_component_reference(val)}
        elif isinstance(val, list) and val and isinstance(val[0], Component):
            val = [{TYPE_METADATA: serialize_component_reference(x)} for x in val]
        elif isinstance(val, BaseQuantity):
            data = val.to_dict()
            data[TYPE_METADATA] = SerializedTypeMetadata(
                fields=SerializedQuantityType(
                    module=val.__module__,
                    type=val.__class__.__name__,
                ),
            ).model_dump()
            val = data
        # TODO: other composite types may need handling.
        # Parent packages can always implement a field_serializer themselves.
        return val

    def pprint(self):
        return _pprint(self)


def raise_if_attached(component: Component):
    """Raise an exception if this component is attached to a system."""
    if component.system_uuid is not None:
        msg = f"{component.label} is attached to system {component.system_uuid}"
        raise ISAlreadyAttached(msg)


def raise_if_not_attached(component: Component, system_uuid: UUID):
    """Raise an exception if this component is not attached to a system.

    Parameters
    ----------
    system_uuid : UUID
        The component must be attached to the system with this UUID.
    """
    if component.system_uuid is None or component.system_uuid != system_uuid:
        msg = f"{component.label} is not attached to the system"
        raise ISNotStored(msg)


def serialize_component_reference(component: Component) -> dict[str, Any]:
    """Make a JSON serializable reference to a component."""
    return SerializedTypeMetadata(
        fields=SerializedComponentReference(
            module=component.__module__,
            type=component.__class__.__name__,
            uuid=component.uuid,
        ),
    ).model_dump(by_alias=True)
