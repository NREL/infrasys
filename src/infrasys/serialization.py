import enum
import importlib
from typing import Any, Literal, Annotated, Type, Union
from uuid import UUID

from pydantic import Field, field_serializer

from infrasys.models import InfraSysBaseModel


TYPE_METADATA = "__metadata__"


class SerializedType(str, enum.Enum):
    """Controls how types are serialized."""

    BASE = "base"
    COMPOSED_COMPONENT = "composed_component"
    QUANTITY = "quantity"


class SerializedTypeBase(InfraSysBaseModel):
    """Applies to all normal types"""

    module: str
    type: str


class SerializedBaseType(SerializedTypeBase):
    """Applies to all normal types"""

    serialized_type: Literal[SerializedType.BASE] = SerializedType.BASE


class SerializedComponentReference(SerializedTypeBase):
    """Reference information for a component that has been serialized as a UUID within another."""

    serialized_type: Literal[SerializedType.COMPOSED_COMPONENT] = SerializedType.COMPOSED_COMPONENT
    uuid: UUID

    @field_serializer("uuid")
    def _serialize_uuid(self, _) -> str:
        return str(self.uuid)


class SerializedQuantityType(SerializedTypeBase):
    serialized_type: Literal[SerializedType.QUANTITY] = SerializedType.QUANTITY


class SerializedTypeMetadata(InfraSysBaseModel):
    """Serializes information about a type so that it can be de-serialized."""

    fields: Annotated[
        Union[
            SerializedBaseType,
            SerializedComponentReference,
            SerializedQuantityType,
        ],
        Field(discriminator="serialized_type"),
    ]


class CachedTypeHelper:
    """Helper class to deserialize types."""

    def __init__(self) -> None:
        self._observed_types: dict[tuple[str, str], Type] = {}
        self._deserialized_types: set[Type] = set()

    def add_deserialized_types(self, types: set[Type]) -> None:
        """Add types that have been deserialized."""
        self._deserialized_types.update(types)

    def allowed_to_deserialize(self, component_type: Type) -> bool:
        """Return True if the type can be deserialized."""
        return component_type in self._deserialized_types

    def get_type(self, metadata: SerializedTypeBase) -> Type:
        """Return the type contained in metadata, dynamically importing as necessary."""
        type_key = (metadata.module, metadata.type)
        component_type = self._observed_types.get(type_key)
        if component_type is None:
            component_type = _deserialize_type(*type_key)
            self._observed_types[type_key] = component_type
        return component_type


def serialize_value(obj: InfraSysBaseModel, *args, **kwargs) -> dict[str, Any]:
    """Serialize an infrasys object to a dictionary."""
    cls = type(obj)
    data = obj.model_dump(*args, mode="json", **kwargs)
    data[TYPE_METADATA] = SerializedTypeMetadata(
        fields=SerializedBaseType(
            module=cls.__module__,
            type=cls.__name__,
        ),
    ).model_dump()
    return data


def deserialize_type(metadata: SerializedTypeBase) -> Type:
    """Dynamically import the type and return it."""
    return _deserialize_type(metadata.module, metadata.type)


def _deserialize_type(module, obj_type) -> Type:
    mod = importlib.import_module(module)
    return getattr(mod, obj_type)


def deserialize_value(data: dict[str, Any], metadata: SerializedTypeBase) -> Any:
    """Deserialize the value from a dictionary."""
    ctype = deserialize_type(metadata)
    return ctype(**data)
