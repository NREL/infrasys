"""Defines a System"""

import importlib
import json
import logging
from collections import defaultdict
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

        # Delegate to the component and time series managers to allow user access directly
        # from the system.
        self.get_components = self._component_mgr.iter
        self.add_component = self._component_mgr.add
        self.add_components = self._component_mgr.add
        self.get_component = self._component_mgr.get
        self.change_component_name = self._component_mgr.change_name
        self.change_component_uuid = self._component_mgr.change_uuid
        self.get_component_by_uuid = self._component_mgr.get_by_uuid
        self.get_components = self._component_mgr.iter
        self.iter_all_components = self._component_mgr.iter_all
        self.list_components_by_name = self._component_mgr.list_by_name
        self.remove_component = self._component_mgr.remove
        self.remove_component_by_name = self._component_mgr.remove_by_name
        self.remove_component_by_uuid = self._component_mgr.remove_by_uuid
        self.add_time_series = self._time_series_mgr.add
        self.get_time_series = self._time_series_mgr.get
        self.get_time_series_by_uuid = self._time_series_mgr.get_by_uuid
        self.remove_time_series = self._time_series_mgr.remove
        self.remove_time_series_by_uuid = self._time_series_mgr.remove_by_uuid
        self.copy_time_series = self._time_series_mgr.copy

        # TODO: add pretty printing of components and time series

    def to_json(self, filename: Path | str, overwrite=False, indent=None) -> None:
        """Write the contents of a system to a JSON file."""
        # TODO: this likely needs to receive more information from the parent class.
        # It will have its own attributes and a data format version. It might be better for
        # this to return a dict back to the parent.
        # TODO: how to get all python package info from environment?
        if isinstance(filename, str):
            filename = Path(filename)
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
    def from_json(cls, filename: Path | str) -> "System":
        """Deserialize a System from a JSON file."""
        with open(filename, encoding="utf-8") as f_in:
            data = json.load(f_in)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "System":
        """Deserialize a System from a dictionary."""
        # TODO: where is the upgrade path handled? parent package?
        sys = cls(name=data.get("name"), uuid=UUID(data["uuid"]))
        sys._deserialize_components(data["components"])
        # TODO: time series storage
        logger.info("Deserialized system %s", sys.summary)
        return sys

    def _deserialize_components(self, components: list[dict[str, Any]]) -> None:
        """Deserialize components from dictionaries and add them to the system."""
        cached_types = _CachedTypeHelper()
        skipped_types = self._deserialize_components_first_pass(components, cached_types)
        if skipped_types:
            self._deserialize_components_nested(skipped_types, cached_types)

    def _deserialize_components_first_pass(
        self, components: list[dict], cached_types: "_CachedTypeHelper"
    ) -> dict:
        deserialized_types = set()
        skipped_types = defaultdict(list)
        for component_dict in components:
            component = self._try_deserialize_component(component_dict, cached_types)
            if component is None:
                component_type = cached_types.get_type(
                    SerializedTypeInfo(**component_dict[TYPE_INFO])
                )
                skipped_types[component_type].append(component_dict)
            else:
                deserialized_types.add(type(component))

        cached_types.add_deserialized_types(deserialized_types)
        return skipped_types

    def _deserialize_components_nested(
        self, skipped_types: dict[Type, list[dict[str, Any]]], cached_types: "_CachedTypeHelper"
    ):
        max_iterations = len(skipped_types)
        for _ in range(max_iterations):
            deserialized_types = set()
            for component_type, components in skipped_types.items():
                component = self._try_deserialize_component(components[0], cached_types)
                if component is None:
                    continue
                if len(components) > 1:
                    for component_dict in components[1:]:
                        component = self._try_deserialize_component(component_dict, cached_types)
                        assert component is not None
                deserialized_types.add(component_type)

            for component_type in deserialized_types:
                skipped_types.pop(component_type)
            cached_types.add_deserialized_types(deserialized_types)

        if skipped_types:
            msg = f"Bug: still have types remaining to be deserialized: {skipped_types.keys()}"
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
