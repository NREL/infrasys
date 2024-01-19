"""Defines base models for components."""

from typing import Any, Literal, Type
from uuid import UUID
from infrasys.base_quantity import BaseQuantity

from loguru import logger
from pydantic import Field, field_serializer
from typing_extensions import Annotated

from infrasys.common import COMPOSED_TYPE_INFO, TYPE_INFO
from infrasys.exceptions import (
    ISNotStored,
    ISOperationNotAllowed,
    ISAlreadyAttached,
)
from infrasys.models import (
    InfraSysBaseModel,
    InfraSysBaseModelWithIdentifers,
    SerializedTypeInfo,
)
from infrasys.time_series_models import (
    TimeSeriesMetadata,
    TimeSeriesMetadataUnion,
)


class Component(InfraSysBaseModelWithIdentifers):
    """Base class for all models representing entities that get attached to a System."""

    name: Annotated[str | None, Field(frozen=True)] = None
    system_uuid: UUID | None = None

    @field_serializer("system_uuid")
    def _serialize_system_uuid(self, _):
        return str(self.system_uuid)

    def check_component_addition(self, system_uuid: UUID):
        """Perform checks on the component before adding it to a system."""

    def is_attached(self, system_uuid: UUID | None = None) -> bool:
        """Return True if the component is attached to a system.

        Parameters
        ----------
        system_uuid : UUID
            Only return True if the component is attached to the system with this UUID.
        """
        if self.system_uuid is None:
            return False
        return self.system_uuid == system_uuid

    def model_dump_custom(self, *args, **kwargs):
        """Custom serialization for this package"""

        refs = {}
        for field in self.model_fields:
            val = getattr(self, field)
            if isinstance(val, Component):
                refs[field] = serialize_component_reference(val)
            elif isinstance(val, list) and val and isinstance(val[0], Component):
                refs[field] = [serialize_component_reference(x) for x in val]
            elif isinstance(val, BaseQuantity):
                refs[field] = val.to_dict()
            # TODO: other composite types may need handling.
            # Parent packages can always implement a field_serializer themselves.

        exclude = kwargs.get("exclude", [])
        exclude += list(set(exclude).union(refs))
        kwargs["exclude"] = exclude
        data = self.model_dump(*args, **kwargs)
        data.update(refs)
        data[TYPE_INFO] = SerializedTypeInfo(
            module=self.__module__, type=self.__class__.__name__
        ).model_dump()
        return data


class ComponentWithQuantities(Component):
    """Base class for all models representing physical components"""

    name: Annotated[str, Field(frozen=True)]
    time_series_metadata: list[TimeSeriesMetadataUnion] = []

    def check_component_addition(self, system_uuid: UUID):
        super().check_component_addition(system_uuid)
        if self.has_time_series():
            msg = f"{self.summary} cannot be added to the system because it has time series." ""
            raise ISOperationNotAllowed(msg)

    def has_time_series(
        self,
        variable_name: str | None = None,
        time_series_type: Type = None,
        **user_attributes,
    ) -> bool:
        """Return True if the component has time series data matching the inputs."""
        return bool(
            self._find_time_series_indexes(
                variable_name, time_series_type=time_series_type, **user_attributes
            )
        )

    def add_time_series_metadata(self, metadata: TimeSeriesMetadata) -> None:
        """Add the metadata to the component. Caller must check for duplicates.

        Raises
        ------
        ISAlreadyAttached
            Raised if the time series is duplicate with another time series attached to the
            component.
        """
        if self.has_time_series(
            variable_name=metadata.variable_name,
            time_series_type=metadata.get_time_series_data_type(),
            **metadata.user_attributes,
        ):
            msg = f"time series {metadata.summary} is already attached to component {self.summary}"
            raise ISAlreadyAttached(msg)

        self.time_series_metadata.append(metadata)
        logger.debug("Added time series %s to %s", metadata.summary, self.summary)

    def get_time_series_metadata(
        self,
        variable_name: str | None = None,
        time_series_type: Type = None,
        **user_attributes,
    ) -> TimeSeriesMetadata:
        """Return the time series metadata matching the inputs.

        Raises
        ------
        ISNotStored
            Raised if no time series match the inputs.
        ISConflictingArguments
            Raised if more than one time series match the inputs.
        """
        indexes = self._find_time_series_indexes(
            variable_name=variable_name, time_series_type=time_series_type, **user_attributes
        )
        if not indexes:
            msg = (
                f"No time series metadata with {time_series_type=} {variable_name=} "
                "{user_attributes=} is attached to {self.summary}"
            )
            raise ISNotStored(msg)

        if len(indexes) > 1:
            msg = (
                f"{len(indexes)} time series match the inputs. "
                "Please refine the filters or call list_time_series_metadata to get all instances"
            )
            raise ISOperationNotAllowed(msg)

        return self.time_series_metadata[indexes[0]]

    def list_time_series_metadata(
        self, variable_name: str | None = None, time_series_type: Type = None, **user_attributes
    ) -> list[TimeSeriesMetadata]:
        """Return the time series metadata matching the inputs."""
        return [
            self.time_series_metadata[i]
            for i in self._find_time_series_indexes(
                variable_name=variable_name, time_series_type=time_series_type, **user_attributes
            )
        ]

    def remove_time_series_metadata(
        self, variable_name: str | None = None, time_series_type: Type = None, **user_attributes
    ) -> list[TimeSeriesMetadata]:
        """Remove and return all time series metadata matching the inputs."""
        indexes = self._find_time_series_indexes(
            variable_name=variable_name, time_series_type=time_series_type, **user_attributes
        )

        if not indexes:
            msg = (
                f"No time series metadata with {time_series_type=} {variable_name=} "
                "{user_attributes=} is attached to {self.summary}"
            )
            raise ISNotStored(msg)

        return [self.time_series_metadata.pop(i) for i in reversed(sorted(indexes))]

    def _find_time_series_indexes(
        self,
        variable_name: str | None = None,
        time_series_type: Type | None = None,
        **user_attributes,
    ) -> list[int]:
        indexes = []
        for i, metadata in enumerate(self.time_series_metadata):
            if (
                time_series_type is not None
                and time_series_type != metadata.get_time_series_data_type()
            ):
                continue
            if variable_name is not None and variable_name != metadata.variable_name:
                continue
            matches = True
            for key, val in user_attributes.items():
                if metadata.user_attributes.get(key) != val:
                    matches = False
                    break
            if matches:
                indexes.append(i)

        return indexes


class SerializedComponentReference(InfraSysBaseModel):
    """Reference information for a component that has been serialized as a UUID within another."""

    composed_type_info: Annotated[Literal[True], Field(default=True, alias=COMPOSED_TYPE_INFO)]
    module: str
    type: str
    uuid: UUID

    @field_serializer("uuid")
    def _serialize_uuid(self, _):
        return str(self.uuid)


def raise_if_attached(component: Component):
    """Raise an exception if this component is attached to a system."""
    if component.system_uuid is not None:
        msg = (
            f"{component.summary} is attached to system %s",
            component.system_uuid,
        )
        raise ISAlreadyAttached(msg)


def raise_if_not_attached(component: Component, system_uuid: UUID):
    """Raise an exception if this component is not attached to a system.

    Parameters
    ----------
    system_uuid : UUID
        The component must be attached to the system with this UUID.
    """
    if component.system_uuid is None or component.system_uuid != system_uuid:
        msg = f"{component.summary} is not attached to the system"
        raise ISNotStored(msg)


def serialize_component_reference(component: Component) -> dict[str, Any]:
    """Make a JSON serializable reference to a component."""
    return SerializedComponentReference(
        module=component.__module__,
        type=component.__class__.__name__,
        uuid=str(component.uuid),
    ).model_dump(by_alias=True)
