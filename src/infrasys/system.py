"""Defines a System"""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Type
from uuid import UUID, uuid4

from loguru import logger

from infrasys.exceptions import (
    ISFileExists,
    ISConflictingArguments,
    ISConflictingSystem,
)
from infrasys.models import make_summary
from infrasys.component_models import (
    Component,
    ComponentWithQuantities,
    raise_if_not_attached,
)
from infrasys.component_manager import ComponentManager
from infrasys.serialization import (
    CachedTypeHelper,
    SerializedTypeMetadata,
    SerializedBaseType,
    SerializedComponentReference,
    SerializedQuantityType,
    SerializedType,
    TYPE_METADATA,
)
from infrasys.time_series_manager import TimeSeriesManager, TIME_SERIES_KWARGS
from infrasys.time_series_models import SingleTimeSeries, TimeSeriesData
from infrasys.utils.json import ExtendedJSONEncoder


class System:
    """Implements behavior for systems"""

    def __init__(
        self,
        name: str | None = None,
        auto_add_composed_components: bool = False,
        time_series_manager: None | TimeSeriesManager = None,
        uuid: UUID | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs a System.

        Parameters
        ----------
        name : str | None
            Optional system name
        auto_add_composed_components : bool
            Set to True to automatically add composed components to the system in add_components.
            The default behavior is to raise an ISOperationNotAllowed when this condition occurs.
            This handles values that are components, such as generator.bus, and lists of
            components, such as subsystem.generators, but not any other form of nested components.
        time_series_manager : None | TimeSeriesManager
            Users should not pass this. De-serialization (from_json) will pass a constructed
            manager.
        kwargs : Any
            Configures time series behaviors:
              - time_series_in_memory: Defaults to true.
              - time_series_read_only: Disables add/remove of time series, defaults to false.
              - time_series_directory: Location to store time series file, defaults to the system's
                tmp directory.

        Examples
        --------
        >>> system = System(name="my_system")
        >>> system2 = System(name="my_system", time_series_directory="/tmp/scratch")
        """
        self._uuid = uuid or uuid4()
        self._name = name
        self._component_mgr = ComponentManager(self._uuid, auto_add_composed_components)
        time_series_kwargs = {k: v for k, v in kwargs.items() if k in TIME_SERIES_KWARGS}
        self._time_series_mgr = time_series_manager or TimeSeriesManager(**time_series_kwargs)
        self._data_format_version: None | str = None

        # TODO: add pretty printing of components and time series

    @property
    def auto_add_composed_components(self) -> bool:
        """Return the setting for auto_add_composed_components."""
        return self._component_mgr.auto_add_composed_components

    @auto_add_composed_components.setter
    def auto_add_composed_components(self, val: bool) -> None:
        """Set auto_add_composed_components."""
        self._component_mgr.auto_add_composed_components = val

    def to_json(self, filename: Path | str, overwrite=False, indent=None, data=None) -> None:
        """Write the contents of a system to a JSON file. Time series will be written to a
        directory at the same level as filename.

        Parameters
        ----------
        filename : Path | str
           Filename to write. If the parent directory does not exist, it will be created.
        overwrite : bool
            Set to True to overwrite the file if it already exists.
        indent : int | None
            Indentation level in the JSON file. Defaults to no indentation.
        data : dict | None
            This is an override for packages that compose this System inside a parent System
            class. If set, it will be the outer object in the JSON file. It must not set the
            key 'system'. Packages that derive a custom instance of this class should leave this
            field unset.

        Examples
        --------
        >>> system.to_json("systems/system1.json")
        INFO: Wrote system data to systems/system1.json
        INFO: Copied time series data to systems/system1_time_series
        """
        # TODO: how to get all python package info from environment?
        if isinstance(filename, str):
            filename = Path(filename)
        if filename.exists() and not overwrite:
            msg = f"{filename=} already exists. Choose a different path or set overwrite=True."
            raise ISFileExists(msg)

        if not filename.parent.exists():
            filename.parent.mkdir()

        time_series_dir = filename.parent / (filename.stem + "_time_series")
        system_data = {
            "name": self.name,
            "uuid": str(self.uuid),
            "data_format_version": self.data_format_version,
            "components": [x.model_dump_custom() for x in self._component_mgr.iter_all()],
            "time_series": {
                # Note: parent directory is stripped. De-serialization will find it from the
                # parent of the JSON file.
                "directory": time_series_dir.name,
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
            logger.info("Wrote system data to {}", filename)

        self._time_series_mgr.serialize(filename.parent / (filename.stem + "_time_series"))

    @classmethod
    def from_json(
        cls, filename: Path | str, upgrade_handler: Callable | None = None, **kwargs
    ) -> "System":
        """Deserialize a System from a JSON file. Refer to System constructor for kwargs.

        Parameters
        ----------
        filename : Path | str
            JSON file containing the system data.
        upgrade_handler : Callable | None
            Optional function to handle data format upgrades. Should only be set when the parent
            package composes this package. If set, it will be called before de-serialization of
            the components.

        Examples
        --------
        >>> system = System.from_json("systems/system1.json")
        """
        with open(filename, encoding="utf-8") as f_in:
            data = json.load(f_in)
        time_series_parent_dir = Path(filename).parent
        return cls.from_dict(
            data, time_series_parent_dir, upgrade_handler=upgrade_handler, **kwargs
        )

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        time_series_parent_dir: Path | str,
        upgrade_handler=None,
        **kwargs,
    ) -> "System":
        """Deserialize a System from a dictionary.

        Parameters
        ----------
        data : dict[str, Any]
            System data in serialized form.
        time_series_parent_dir : Path | str
            Directory that contains the system's time series directory.
        upgrade_handler : Callable | None
            Optional function to handle data format upgrades. Should only be set when the parent
            package composes this package. If set, it will be called before de-serialization of
            the components.

        Examples
        --------
        >>> system = System.from_dict(data, "systems")
        """
        system_data = data if "system" not in data else data["system"]
        ts_kwargs = {k: v for k, v in kwargs.items() if k in TIME_SERIES_KWARGS}
        time_series_manager = TimeSeriesManager.deserialize(
            data["time_series"], time_series_parent_dir, **ts_kwargs
        )
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

        Examples
        --------
        >>> system.add_component(Bus.example())

        See Also
        --------
        add_components
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

        Examples
        --------
        >>> system.add_components(Bus.example(), Generator.example())

        See Also
        --------
        add_component
        """
        return self._component_mgr.add(*components, **kwargs)

    def change_component_uuid(self, component: Component) -> None:
        """Change the component UUID. This is required if you copy a component and attach it to
        the same system.

        Parameters
        ----------
        component : Component
        """
        return self._component_mgr.change_uuid(component)

    def copy_component(
        self,
        component: Type,
        name: str | None = None,
        attach: bool = False,
    ) -> Any:
        """Create a copy of the component. Time series data is excluded.The new component will
        have a different UUID from the original.

        Parameters
        ----------
        component : Type
            Type of the source component
        name : str
            Optional, if None, keep the original name.
        attach : bool
            Optional, if True, attach the new component to the system.

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> gen2 = system.copy_component(gen, name="gen2")
        >>> gen3 = system.copy_component(gen, name="gen3", attach=True)
        """
        return self._component_mgr.copy(component, name=name, attach=attach)

    def get_component(self, component_type: Type, name: str) -> Any:
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

        Examples
        --------
        >>> system.get_component(Generator, "gen1")

        See Also
        --------
        list_by_name
        """
        return self._component_mgr.get(component_type, name)

    def get_component_by_uuid(self, uuid: UUID) -> Any:
        """Return the component with the input UUID.

        Parameters
        ----------
        uuid : UUID

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored.

        Examples
        --------
        >>> uuid = UUID("714c8311-8dff-4ae2-aa2e-30779a317d42")
        >>> component = system.get_component_by_uuid(uuid)
        """
        return self._component_mgr.get_by_uuid(uuid)

    def get_components(
        self, component_type: Type, filter_func: Callable | None = None
    ) -> Iterable[Any]:
        """Return the components with the passed type and that optionally match filter_func.

        Parameters
        ----------
        component_type : Type
            If component_type is an abstract type, all matching subtypes will be returned.
        filter_func : Callable | None
            Optional function to filter the returned values. The function must accept a component
            as a single argument.

        Examples
        --------
        >>> for component in system.get_components(Component)
            print(component.summary)
        >>> names = {"bus1", "bus2", "gen1", "gen2"}
        >>> for component in system.get_components(
            Component,
            filter_func=lambda x: x.name in names,
        ):
            print(component.summary)
        """
        return self._component_mgr.iter(component_type, filter_func=filter_func)

    def get_component_types(self) -> Iterable[Type]:
        """Return an iterable of all component types stored in the system.

        Examples
        --------
        >>> for component_type in system.get_component_types():
            print(component_type)
        """
        return self._component_mgr.get_types()

    def list_components_by_name(self, component_type: Type, name: str) -> list[Any]:
        """Return all components that match component_type and name.

        Parameters
        ----------
        component_type : Type
        name : str

        Examples
        --------
        system.list_components_by_name(Generator, "gen1")
        """
        return self._component_mgr.list_by_name(component_type, name)

    def iter_all_components(self) -> Iterable[Any]:
        """Return an iterator over all components.

        Examples
        --------
        >>> for component in system.iter_all_components()
            print(component.summary)

        See Also
        --------
        get_components
        """
        return self._component_mgr.iter_all()

    def remove_component(self, component: Any) -> Any:
        """Remove the component from the system and return it.

        Parameters
        ----------
        component : Component

        Raises
        ------
        ISNotStored
            Raised if the component is not stored in the system.

        Examples
        --------
        >>> gen = system.get_component(Generator, "gen1")
        >>> system.remove_component(gen)
        """
        raise_if_not_attached(component, self.uuid)
        if component.has_time_series():
            for metadata in component.list_time_series_metadata():
                self.remove_time_series(
                    component,
                    time_series_type=metadata.get_time_series_data_type(),
                    variable_name=metadata.variable_name,
                    **metadata.user_attributes,
                )
        component = self._component_mgr.remove(component)

    def remove_component_by_name(self, component_type: Type, name: str) -> list[Any]:
        """Remove all components matching the inputs from the system and return them.

        Parameters
        ----------
        component_type : Type
        name : str

        Raises
        ------
        ISNotStored
            Raised if the inputs do not match any components in the system.
        ISOperationNotAllowed
            Raised if there is more than one component with component type and name.

        Examples
        --------
        >>> generators = system.remove_by_name(Generator, "gen1")
        """
        component = self.get_component(component_type, name)
        return self.remove_component(component)

    def remove_component_by_uuid(self, uuid: UUID) -> Any:
        """Remove the component with uuid from the system and return it.

        Parameters
        ----------
        uuid : UUID

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored in the system.

        Examples
        --------
        >>> uuid = UUID("714c8311-8dff-4ae2-aa2e-30779a317d42")
        >>> generator = system.remove_component_by_uuid(uuid)
        """
        component = self.get_component_by_uuid(uuid)
        return self.remove_component(component)

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

        Examples
        --------
        >>> system.update_components(Generator, lambda x: x.active_power *= 10)
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

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> gen2 = system.get_component(Generator, "gen2")
        >>> ts = SingleTimeSeries.from_array(
            data=[0.86, 0.78, 0.81, 0.85, 0.79],
            variable_name="active_power",
            start_time=datetime(year=2030, month=1, day=1),
            resolution=timedelta(hours=1),
        )
        >>> system.add_time_series(ts, gen1, gen2)
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

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> gen2 = system.get_component(Generator, "gen2")
        >>> system.copy_time_series(gen1, gen2)
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
    ) -> Any:
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

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> ts_full = system.get_time_series(gen1, "active_power")
        >>> ts_slice = system.get_time_series(
            gen1,
            "active_power",
            start_time=datetime(year=2030, month=1, day=1, hour=5),
            length=5,
        )

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

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> for ts in system.list_time_series(gen1):
            print(ts)
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

        Examples
        --------
        >>> gen1 = system.get_component(Generator, "gen1")
        >>> system.remove_time_series(gen1, "active_power")
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
        cached_types = CachedTypeHelper()
        skipped_types = self._deserialize_components_first_pass(components, cached_types)
        if skipped_types:
            self._deserialize_components_nested(skipped_types, cached_types)

    def _deserialize_components_first_pass(
        self, components: list[dict], cached_types: CachedTypeHelper
    ) -> dict:
        deserialized_types = set()
        skipped_types = defaultdict(list)
        for component_dict in components:
            component = self._try_deserialize_component(component_dict, cached_types)
            if component is None:
                metadata = SerializedTypeMetadata(**component_dict[TYPE_METADATA])
                assert isinstance(metadata.fields, SerializedBaseType)
                component_type = cached_types.get_type(metadata.fields)
                skipped_types[component_type].append(component_dict)
            else:
                deserialized_types.add(type(component))

        cached_types.add_deserialized_types(deserialized_types)
        return skipped_types

    def _deserialize_components_nested(
        self,
        skipped_types: dict[Type, list[dict[str, Any]]],
        cached_types: CachedTypeHelper,
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

    def _try_deserialize_component(self, component: dict, cached_types: CachedTypeHelper) -> Any:
        actual_component = None
        values = self._deserialize_fields(component, cached_types)
        if values is None:
            return None

        metadata = SerializedTypeMetadata(**component[TYPE_METADATA])
        component_type = cached_types.get_type(metadata.fields)
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

    def _deserialize_fields(self, component: dict, cached_types: CachedTypeHelper) -> dict | None:
        values = {}
        for field, value in component.items():
            if isinstance(value, dict) and TYPE_METADATA in value:
                metadata = SerializedTypeMetadata(**value[TYPE_METADATA])
                if isinstance(metadata.fields, SerializedComponentReference):
                    composed_value = self._deserialize_composed_value(
                        metadata.fields, cached_types
                    )
                    if composed_value is None:
                        return None
                    values[field] = composed_value
                elif isinstance(metadata.fields, SerializedQuantityType):
                    quantity_type = cached_types.get_type(metadata.fields)
                    values[field] = quantity_type.from_dict(value)
                else:
                    msg = f"Bug: unhandled type: {field=} {value=}"
                    raise NotImplementedError(msg)
            elif (
                isinstance(value, list)
                and value
                and isinstance(value[0], dict)
                and TYPE_METADATA in value[0]
                and value[0][TYPE_METADATA]["fields"]["serialized_type"]
                == SerializedType.COMPOSED_COMPONENT.value
            ):
                metadata = SerializedTypeMetadata(**value[0][TYPE_METADATA])
                assert isinstance(metadata.fields, SerializedComponentReference)
                composed_values = self._deserialize_composed_list(value, cached_types)
                if composed_values is None:
                    return None
                values[field] = composed_values
            elif field != TYPE_METADATA:
                values[field] = value

        return values

    def _deserialize_composed_value(
        self, metadata: SerializedComponentReference, cached_types: CachedTypeHelper
    ) -> Any:
        component_type = cached_types.get_type(metadata)
        if cached_types.allowed_to_deserialize(component_type):
            return self.components.get_by_uuid(metadata.uuid)
        return None

    def _deserialize_composed_list(
        self, components: list[dict], cached_types: CachedTypeHelper
    ) -> list[Any] | None:
        deserialized_components = []
        for component in components:
            metadata = SerializedTypeMetadata(**component[TYPE_METADATA])
            assert isinstance(metadata.fields, SerializedComponentReference)
            component_type = cached_types.get_type(metadata.fields)
            if cached_types.allowed_to_deserialize(component_type):
                deserialized_components.append(self.components.get_by_uuid(metadata.fields.uuid))
            else:
                return None
        return deserialized_components
