"""Defines a System"""

import importlib
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Type
from uuid import UUID, uuid4

from loguru import logger

from infrasys.common import COMPOSED_TYPE_INFO, TYPE_INFO
from infrasys.exceptions import (
    ISFileExists,
    ISConflictingArguments,
    ISConflictingSystem,
)
from infrasys.models import SerializedTypeInfo, make_summary
from infrasys.component_models import (
    Component,
    ComponentWithQuantities,
    SerializedComponentReference,
)
from infrasys.component_manager import ComponentManager
from infrasys.time_series_manager import TimeSeriesManager, TIME_SERIES_KWARGS
from infrasys.time_series_models import SingleTimeSeries, TimeSeriesData
from infrasys.utils.json import ExtendedJSONEncoder


class System:
    """Implements behavior for systems"""

    def __init__(
        self,
        name: str | None = None,
        time_series_manager: None | TimeSeriesManager = None,
        uuid: UUID | None = None,
        **kwargs,
    ):
        """Constructs a System.

        Parameters
        ----------
        name : str | None
            Optional system name
        time_series_manager : None | TimeSeriesManager
            Users should not pass this. De-serialization (from_json) will pass a constructed
            manager.
        kwargs
            Configures time series behaviors:
              - time_series_in_memory: Defaults to true.
              - time_series_read_only: Disables add/remove of time series, defaults to false.
              - time_series_directory: Location to store time series file, defaults to the system's
                tmp directory.
        """
        self._uuid = uuid or uuid4()
        self._name = name
        self._component_mgr = ComponentManager(self._uuid)
        time_series_kwargs = {k: v for k, v in kwargs.items() if k in TIME_SERIES_KWARGS}
        self._time_series_mgr = time_series_manager or TimeSeriesManager(**time_series_kwargs)
        self._data_format_version = None

        # TODO: add pretty printing of components and time series

    def to_json(self, filename: Path | str, overwrite=False, indent=None, data=None) -> None:
        """Write the contents of a system to a JSON file."""
        # TODO: how to get all python package info from environment?
        if isinstance(filename, str):
            filename = Path(filename)
        if filename.exists() and not overwrite:
            msg = f"{filename=} already exists. Choose a different path or set overwrite=True."
            raise ISFileExists(msg)

        system_data = {
            "name": self.name,
            "uuid": str(self.uuid),
            "data_format_version": self.data_format_version,
            "components": [x.model_dump_custom() for x in self._component_mgr.iter_all()],
            "time_series": {
                "directory": filename.parent / (filename.name + "_time_series"),
            },
        }
        extra = self.serialize_system_attributes()
        intersection = set(extra).intersection(system_data)
        if intersection:
            msg = f"Extra attributes from parent class collide with System: {intersection}"
            raise ISConflictingArguments(msg)
        system_data.update(extra)

        if data is None:
            data = system_data
        else:
            if "system" in data:
                raise ISConflictingArguments("data contains the key 'system'")
            data["system"] = system_data
        with open(filename, "w", encoding="utf-8") as f_out:
            json.dump(data, f_out, indent=indent, cls=ExtendedJSONEncoder)

        self._time_series_mgr.serialize(filename.parent / (filename.name + "_time_series"))

    @classmethod
    def from_json(cls, filename: Path | str, upgrade_handler=None, **kwargs) -> "System":
        """Deserialize a System from a JSON file. Refer to System constructor for kwargs."""
        with open(filename, encoding="utf-8") as f_in:
            data = json.load(f_in)
        return cls.from_dict(data, upgrade_handler=upgrade_handler, **kwargs)

    @classmethod
    def from_dict(cls, data: dict[str, Any], upgrade_handler=None, **kwargs) -> "System":
        """Deserialize a System from a dictionary."""
        system_data = data if "system" not in data else data["system"]
        ts_kwargs = {k: v for k, v in kwargs.items() if k in TIME_SERIES_KWARGS}
        time_series_manager = TimeSeriesManager.deserialize(data["time_series"], **ts_kwargs)
        system = cls(
            name=system_data.get("name"),
            time_series_manager=time_series_manager,
            uuid=UUID(system_data["uuid"]),
            **kwargs,
        )
        if system_data.get("data_format_version") != system.data_format_version:
            # This handles the case where the parent package inherited from System.
            system.handle_data_format_upgrade(
                system_data,
                system_data.get("data_format_version"),
                system.data_format_version,
            )
            # This handles the case where the parent package composes an instance of System.
            if upgrade_handler is not None:
                upgrade_handler(
                    system_data,
                    system_data.get("data_format_version"),
                    system.data_format_version,
                )
        system.deserialize_system_attributes(system_data)
        system._deserialize_components(system_data["components"])
        logger.info("Deserialized system {}", system.summary)
        return system

    def add_component(self, component: Component, **kwargs) -> None:
        """Add one component to the system.

        Parameters
        ----------
        component : Component
            Component to add to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        return self.add_components(component, **kwargs)

    def add_components(self, *components: Component, **kwargs) -> None:
        """Add one or more components to the system.

        Parameters
        ----------
        components : Component
            Component(s) to add to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        return self._component_mgr.add(*components, **kwargs)

    def change_component_uuid(self, component: Component) -> None:
        """Change the component UUID."""
        return self._component_mgr.change_uuid(component)

    def copy_component(
        self,
        component: Type,
        new_name: str,
        attach_to_system: bool = False,
        copy_time_series: bool = True,
    ) -> Component:
        """Create a copy of the component.

        Parameters
        ----------
        component : Type
            Type of the source component
        new_name : str
            Name of the new component
        attach_to_system : bool
            Optional, if True, attach the new component to the system.
        copy_time_series : bool
            Optional, if True, copy all time series to the new component.
        """
        return self._component_mgr.copy(
            component,
            new_name,
            attach_to_system=attach_to_system,
            copy_time_series=copy_time_series,
        )

    def get_component(self, component_type: Type, name: str) -> Component:
        """Return the component with the passed type and name.

        Parameters
        ----------
        component_type : Type
            Type of component
        name : Type
            Name of component

        Raises
        ------
        ISDuplicateNames
            Raised if more than one component match the inputs.

        See Also
        --------
        list_by_name
        """
        return self._component_mgr.get(component_type, name)

    def get_component_by_uuid(self, uuid: UUID) -> Component:
        """Return the component with the input UUID.

        Parameters
        ----------
        uuid : UUID

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored.
        """
        return self._component_mgr.get_by_uuid(uuid)

    def get_components(
        self, component_type: Type, filter_func: Callable | None = None
    ) -> Iterable[Component]:
        """Return the components with the passed type and optionally match filter_func.

        Parameters
        ----------
        component_type : Type
            If component_type is an abstract type, all matching subtypes will be returned.
        filter_func : Callable | None
            Optional function to filter the returned values. The function must accept a component
            as a single argument.
        """
        return self._component_mgr.iter(component_type, filter_func=filter_func)

    def list_components_by_name(self, component_type: Type, name: str) -> list[Component]:
        """Return all components that match component_type and name.

        Parameters
        ----------
        component_type : Type
        name : str
        """
        return self._component_mgr.list_by_name(component_type, name)

    def iter_all_components(self) -> Iterable[Component]:
        """Return an iterator over all components."""
        return self._component_mgr.iter_all()

    def remove_component(self, component: Component) -> Component:
        """Remove the component from the system and return it.

        Parameters
        ----------
        component : Component

        Raises
        ------
        ISNotStored
            Raised if the component is not stored in the system.
        """
        return self._component_mgr.remove(component)

    def remove_component_by_name(self, component_type: Type, name: str) -> list[Component]:
        """Remove all components matching the inputs from the system and return them.

        Parameters
        ----------
        component_type : Type
        name : str

        Raises
        ------
        ISNotStored
            Raised if the inputs do not match any components in the system.
        """
        return self._component_mgr.remove_by_name(component_type, name)

    def remove_component_by_uuid(self, uuid: UUID) -> Component:
        """Remove the components with uuid from the system and return it.

        Parameters
        ----------
        uuid : UUID

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored in the system.
        """
        return self._component_mgr.remove_by_uuid(uuid)

    def update_components(
        self,
        component_type: Type,
        update_func: Callable,
        filter_func: Callable | None = None,
    ) -> None:
        """Update multiple components of a given type.

        Parameters
        ----------
        component_type : Type
            Type of component to update. Can be abstract.
        update_func : Callable
            Function to call on each component. Must take a component as a single argument.
        filter_func : Callable | None
            Optional function to filter the components to update. Must take a component as a
            single argument.
        """
        return self._component_mgr.update(component_type, update_func, filter_func=filter_func)

    def add_time_series(
        self,
        time_series: TimeSeriesData,
        *components: ComponentWithQuantities,
        **user_attributes: Any,
    ) -> None:
        """Store a time series array for one or more components.

        Parameters
        ----------
        time_series : TimeSeriesData
            Time series data to store.
        components : ComponentWithQuantities
            Add the time series to all of these components.
        user_attributes : Any
            Key/value pairs to store with the time series data. Must be JSON-serializable.

        Raises
        ------
        ISAlreadyAttached
            Raised if the variable name and user attributes match any time series already
            attached to one of the components.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        return self._time_series_mgr.add(time_series, *components, **user_attributes)

    def copy_time_series(
        self,
        dst: ComponentWithQuantities,
        src: ComponentWithQuantities,
        name_mapping: dict[str, str] | None = None,
    ) -> None:
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst : ComponentWithQuantities
            Destination component
        src : ComponentWithQuantities
            Source component
        name_mapping : dict[str, str]
            Optionally map src names to different dst names.
            If provided and src has a time_series with a name not present in name_mapping, that
            time_series will not copied. If name_mapping is nothing then all time_series will be
            copied with src's names.

        Notes
        -----
        name_mapping is currently not implemented.
        """
        return self._time_series_mgr.copy(dst, src, name_mapping=name_mapping)

    def get_time_series(
        self,
        component: ComponentWithQuantities,
        variable_name: str | None = None,
        time_series_type: Type = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes: str,
    ) -> TimeSeriesData:
        """Return a time series array.

        Parameters
        ----------
        component : ComponentWithQuantities
            Return time series attached to this component.
        variable_name : str | None
            Optional, return time series with this name.
        time_series_type : Type
            Optional, return time series with this type.
        start_time : datetime | None
            Return a slice of the time series starting at this time. Defaults to the first value.
        length : int | None
            Return a slice of the time series with this length. Defaults to the full length.
        user_attributes : str
            Return time series with these attributes.

        Raises
        ------
        ISNotStored
            Raised if no time series matches the inputs.
            Raised if the inputs match more than one time series.
        ISOperationNotAllowed
            Raised if the inputs match more than one time series.

        See Also
        --------
        list_time_series
        """
        return self._time_series_mgr.get(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type,
            start_time=start_time,
            length=length,
            **user_attributes,
        )

    def list_time_series(
        self,
        component: ComponentWithQuantities,
        variable_name: str | None = None,
        time_series_type: Type = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes,
    ) -> list[TimeSeriesData]:
        """Return all time series that match the inputs.

        Parameters
        ----------
        component : ComponentWithQuantities
            Return time series attached to this component.
        variable_name : str | None
            Optional, return time series with this name.
        time_series_type : Type
            Optional, return time series with this type.
        start_time : datetime | None
            Return a slice of the time series starting at this time. Defaults to the first value.
        length : int | None
            Return a slice of the time series with this length. Defaults to the full length.
        user_attributes : str
            Return time series with these attributes.
        """
        return self._time_series_mgr.list_time_series(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type,
            start_time=start_time,
            length=length,
            **user_attributes,
        )

    def remove_time_series(
        self,
        *components: ComponentWithQuantities,
        variable_name: str | None = None,
        time_series_type: Type = SingleTimeSeries,
        **user_attributes,
    ):
        """Remove all time series arrays attached to the components matching the inputs.

        Parameters
        ----------
        components : ComponentWithQuantities
            Affected components
        variable_name : str | None
            Optional, defaults to any name.
        time_series_type : Type | None
            Optional, defaults to any type.
        user_attributes : str
            Remove only time series with these attributes.
        Raises
        ------
        ISNotStored
            Raised if no time series match the inputs.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        return self._time_series_mgr.remove(
            *components,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )

    def serialize_system_attributes(self) -> dict[str, Any]:
        """Allows subclasses to serialize attributes at the root level."""
        return {}

    def deserialize_system_attributes(self, data: dict[str, Any]) -> None:
        """Allows subclasses to deserialize attributes stored in the JSON at the root level.

        The method should modify self with its custom attributes in data.
        """

    def handle_data_format_upgrade(self, data: dict[str, Any], from_version, to_version) -> None:
        """Allows subclasses to upgrade data models.

        The parameter data contains the full contents of the serialized JSON file.
        The method should modify the data models in-place.
        """

    def merge_system(self, other: "System") -> None:
        """Merge the contents of another system into this one."""
        raise NotImplementedError("merge_system")

    # TODO: add delete methods that (1) don't raise if not found and (2) don't return anything?

    @property
    def components(self) -> ComponentManager:
        """Return the component manager."""
        return self._component_mgr

    @property
    def data_format_version(self) -> str | None:
        """Return the data format version of the component models."""
        return self._data_format_version

    @data_format_version.setter
    def data_format_version(self, data_format_version: str) -> None:
        """Set the data format version for the component models."""
        self._data_format_version = data_format_version

    @property
    def name(self):
        """Return the name of the system."""
        return self._name

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
        self,
        skipped_types: dict[Type, list[dict[str, Any]]],
        cached_types: "_CachedTypeHelper",
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
        actual_component = None
        values = self._deserialize_fields(component, cached_types)
        if values is None:
            return None

        component_type = cached_types.get_type(SerializedTypeInfo(**component[TYPE_INFO]))
        system_uuid = values.pop("system_uuid")
        if str(self.uuid) != system_uuid:
            msg = (
                "component has a system_uuid that conflicts with the system: "
                f"{values} component's system_uuid={system_uuid} system={self.uuid}"
            )
            raise ISConflictingSystem(msg)
        actual_component = component_type(**values)
        self.components.add(actual_component, deserialization_in_progress=True)
        if actual_component.has_time_series():
            # This allows the time series manager to rebuild the reference counts of time
            # series and then manage deletions.
            self.time_series.add_reference_counts(actual_component)

        return actual_component

    def _deserialize_fields(self, component: dict, cached_types) -> dict | None:
        values = {}
        for field, value in component.items():
            if isinstance(value, dict) and COMPOSED_TYPE_INFO in value:
                composed_value = self._deserialize_composed_value(value, cached_types)
                if composed_value is None:
                    return None
                values[field] = composed_value
            elif (
                isinstance(value, list)
                and value
                and isinstance(value[0], dict)
                and COMPOSED_TYPE_INFO in value[0]
            ):
                composed_values = self._deserialize_composed_list(value, cached_types)
                if composed_values is None:
                    return None
                values[field] = composed_values
            elif isinstance(value, dict) and TYPE_INFO in value:
                values[field] = cached_types.get_type(
                    SerializedTypeInfo(**value[TYPE_INFO])
                ).from_dict(value)
            elif field != TYPE_INFO:
                values[field] = value

        return values

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

    def get_type(self, ref: SerializedComponentReference | SerializedTypeInfo):
        """Return the type contained in ref, dynamically importing as necessary."""
        type_key = (ref.module, ref.type)
        component_type = self._observed_types.get(type_key)
        if component_type is None:
            mod = importlib.import_module(type_key[0])
            component_type = getattr(mod, type_key[1])
            self._observed_types[type_key] = component_type
        return component_type
