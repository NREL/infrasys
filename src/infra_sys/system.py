"""Defines a System"""

import importlib
import json
import logging
from pathlib import Path
from typing import Any, Type
from uuid import UUID, uuid4

from infra_sys.common import COMPOSED_TYPE_INFO, TYPE_INFO
from infra_sys.exceptions import (
    ISFileExists,
    ISConflictingSystem,
)
from infra_sys.models import SerializedTypeInfo, make_summary
from infra_sys.component_models import (
    Component,
    SerializedComponentReference,
)
from infra_sys.component_manager import ComponentManager
from infra_sys.time_series_manager import TimeSeriesManager
from infra_sys.time_series_storage_base import TimeSeriesStorageBase

logger = logging.getLogger(__name__)


class System:
    """Implements behavior for systems"""

    def __init__(
        self,
        name: str | None = None,
        time_series_storage: TimeSeriesStorageBase | None = None,
        uuid: UUID | None = None,
    ):
        self._uuid = uuid or uuid4()
        self._name = name
        self._component_mgr = ComponentManager(self._uuid)
        self._time_series_mgr = TimeSeriesManager(storage=time_series_storage)
        # TODO: add pretty printing of components and time series

    def to_json(self, filename: Path, overwrite=False, indent=None) -> None:
        """Write the contents of a system to a JSON file."""
        # TODO: this likely needs to receive more information from the parent class.
        # It will have its own attributes and a data format version. It might be better for
        # this to return a dict back to the parent.
        # TODO: how to get all python package info from environment?
        if filename.exists() and not overwrite:
            msg = f"{filename=} already exists. Choose a different path or set overwrite=True."
            raise ISFileExists(msg)
        data = {
            "name": self.name,
            "uuid": str(self.uuid),
            "components": [x.model_dump_custom() for x in self.components.iter_all()],
        }
        with open(filename, "w", encoding="utf-8") as f_out:
            json.dump(data, f_out, indent=indent)

    @classmethod
    def from_json(cls, filename: Path) -> "System":
        """Deserialize a System from a JSON file."""
        with open(filename, encoding="utf-8") as f_in:
            data = json.load(f_in)

        # TODO: where is the upgrade path handled? parent package?
        sys = cls(name=data.get("name"), uuid=UUID(data["uuid"]))
        sys.deserialize_components(data["components"])
        # TODO: time series storage
        logger.info("Deserialized system %s", sys.summary)
        return sys

    def deserialize_components(self, components: list[dict[str, Any]]) -> None:
        """Deserialize components from dictionaries and add them to the system."""
        remaining_component_indexes = list(range(len(components)))
        cached_types = _CachedTypeHelper()
        max_iterations = len(components)

        for _ in range(max_iterations):
            deserialized_types_this_round = set()
            for i in range(len(remaining_component_indexes), 0, -1):
                index = i - 1
                component_dict = components[remaining_component_indexes[index]]
                component = self._try_deserialize_component(component_dict, cached_types)
                if component is not None:
                    remaining_component_indexes.pop(index)
                    deserialized_types_this_round.add(type(component))

            cached_types.add_deserialized_types(deserialized_types_this_round)
            if not remaining_component_indexes:
                break

        if remaining_component_indexes:
            msg = f"Bug: Failed to deserialize these indexes: {remaining_component_indexes}"
            raise Exception(msg)

    def _try_deserialize_component(
        self, component: dict, cached_types: "_CachedTypeHelper"
    ) -> Component | None:
        values = {}
        can_be_deserialized = True
        for field, value in component.items():
            if isinstance(value, dict) and COMPOSED_TYPE_INFO in value:
                composed_value = self._deserialize_composed_value(value, cached_types)
                if composed_value is None:
                    can_be_deserialized = False
                    break
                values[field] = composed_value
            elif isinstance(value, list) and value and COMPOSED_TYPE_INFO in value[0]:
                composed_values = self._deserialize_composed_list(value, cached_types)
                if composed_values is None:
                    can_be_deserialized = False
                    break
                values[field] = composed_values
            elif field != TYPE_INFO:
                values[field] = value

        actual_component = None
        if can_be_deserialized:
            component_type = cached_types.get_type(SerializedTypeInfo(**component[TYPE_INFO]))
            system_uuid = values.pop("system_uuid")
            if str(self.uuid) != system_uuid:
                msg = (
                    "component has a system_uuid that conflicts with the system: "
                    f"{values} component's system_uuid={system_uuid} system={self.uuid}"
                )
                raise ISConflictingSystem(msg)
            actual_component = component_type(**values)
            self.components.add(actual_component)

        return actual_component

    def _deserialize_composed_value(self, value: dict, cached_types) -> Component | None:
        ref = SerializedComponentReference(**value)
        component_type = cached_types.get_type(ref)
        if cached_types.allowed_to_deserialize(component_type):
            return self.components.get_by_uuid(ref.uuid)
        return None

    def _deserialize_composed_list(
        self, components: list[dict], cached_types
    ) -> list[Component] | None:
        deserialized_components = []
        for component in components:
            ref = SerializedComponentReference(**component)
            component_type = cached_types.get_type(ref)
            if cached_types.allowed_to_deserialize(component_type):
                deserialized_components.append(self.components.get_by_uuid(ref.uuid))
            else:
                return None
        return deserialized_components

    @property
    def name(self):
        """Return the name of the system."""
        return self._name

    @property
    def components(self) -> TimeSeriesManager:
        """Return the component manager."""
        return self._component_mgr

    @property
    def summary(self) -> str:
        """Provides a description of the system."""
        name = self.name or str(self.uuid)
        return make_summary(self.__class__.__name__, name)

    @property
    def time_series(self) -> TimeSeriesManager:
        """Return the time series manager."""
        return self._time_series_mgr

    @property
    def uuid(self):
        """Return the UUID of the system."""
        return self._uuid

    def merge_system(self, other: "System") -> None:
        """Merge the contents of another system into this one."""
        raise NotImplementedError("merge_system")

    # TODO: add delete methods that (1) don't raise if not found and (2) don't return anything?


class _CachedTypeHelper:
    def __init__(self):
        self._observed_types = {}
        self._deserialized_types: set[Type] = set()

    def add_deserialized_types(self, types: set[Type]):
        """Add types that have been deserialized."""
        self._deserialized_types.update(types)

    def allowed_to_deserialize(self, component_type: Type) -> bool:
        """Return True if the type can be deserialized."""
        return component_type in self._deserialized_types

    def get_type(self, ref: SerializedComponentReference):
        """Return the type contained in ref, dynamically importing as necessary."""
        type_key = (ref.module, ref.type)
        component_type = self._observed_types.get(type_key)
        if component_type is None:
            mod = importlib.import_module(type_key[0])
            component_type = getattr(mod, type_key[1])
            self._observed_types[type_key] = component_type
        return component_type
