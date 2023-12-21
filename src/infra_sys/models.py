"""Base models for the package"""

import abc
import logging
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_serializer


logger = logging.getLogger(__name__)


def make_model_config(**kwargs) -> ConfigDict:
    """Return a Pydantic config"""
    return ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        validate_default=True,
        extra="forbid",
        use_enum_values=False,
        arbitrary_types_allowed=True,
        populate_by_name=True,
        **kwargs,
    )


class InfraSysBaseModel(BaseModel):
    """Base class for all Infrastructure Systems models"""

    model_config = make_model_config()


class InfraSysBaseModelWithIdentifers(InfraSysBaseModel, abc.ABC):
    """Base class for all Infrastructure Systems types with names and UUIDs"""

    uuid: UUID = Field(default_factory=uuid4)

    @field_serializer("uuid")
    def _serialize_uuid(self, _):
        return str(self.uuid)

    def assign_new_uuid(self):
        """Generate a new UUID."""
        self.uuid = uuid4()
        logger.debug("Assigned new UUID for %s: %s", self.summary, self.uuid)

    @classmethod
    def example(cls) -> "InfraSysBaseModelWithIdentifers":
        """Return an example instance of the model.

        Raises
        ------
        NotImplementedError
            Raised if the model does not implement this method.
        """
        msg = f"{cls.model_json_schema()['title']} does not implement example()"
        raise NotImplementedError(msg)

    @property
    def summary(self) -> str:
        """Provides a description of an instance."""
        class_name = self.__class__.__name__
        name = getattr(self, "name", None) or str(self.uuid)
        return make_summary(class_name, name)


class SerializedTypeInfo(InfraSysBaseModel):
    """Defines the type of a serialized object."""

    module: str
    type: str


def make_summary(class_name: str, name: str) -> str:
    """Make a string summarizing an instance."""
    return f"{class_name}.{name}"
