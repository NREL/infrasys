"""Manages components"""

import itertools
from typing import Any, Callable, Iterable, Type
from uuid import UUID
from loguru import logger

from infrasys.component_models import Component, raise_if_attached
from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.models import make_summary


class ComponentManager:
    """Manages components"""

    def __init__(
        self,
        uuid: UUID,
        auto_add_composed_components: bool,
    ) -> None:
        self._components: dict[Type, dict[str | None, list[Component]]] = {}
        self._components_by_uuid: dict[UUID, Component] = {}
        self._uuid = uuid
        self._auto_add_composed_components = auto_add_composed_components

    @property
    def auto_add_composed_components(self) -> bool:
        """Return the setting for auto_add_composed_components."""
        return self._auto_add_composed_components

    @auto_add_composed_components.setter
    def auto_add_composed_components(self, val: bool) -> None:
        """Set auto_add_composed_components."""
        self._auto_add_composed_components = val

    def add(self, *args: Component, deserialization_in_progress=False) -> None:
        """Add one or more components to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        for component in args:
            self._add(component, deserialization_in_progress)

    def get(self, component_type: Type[Component], name: str) -> Any:
        """Return the component with the passed type and name.

        Raises
        ------
        ISDuplicateNames
            Raised if more than one component match the inputs.

        See Also
        --------
        list_by_name
        """
        if component_type not in self._components or name not in self._components[component_type]:
            summary = make_summary(str(component_type), name)
            msg = f"{summary} is not stored"
            raise ISNotStored(msg)

        components = self._components[component_type][name]
        if len(components) > 1:
            msg = (
                f"There is more than one {component_type} with {name=}. Please use "
                "list_by_name instead."
            )
            raise ISOperationNotAllowed(msg)

        assert components
        return components[0]

    def get_types(self) -> Iterable[Type[Component]]:
        """Return an iterable of all stored types."""
        return self._components.keys()

    def iter(
        self, component_type: Type[Component], filter_func: Callable | None = None
    ) -> Iterable[Any]:
        """Return the components with the passed type and optionally match filter_func.

        If component_type is an abstract type, all matching subtypes will be returned.
        """
        yield from self._iter(component_type, filter_func)

    def _iter(
        self, component_type: Type[Component], filter_func: Callable | None
    ) -> Iterable[Any]:
        subclasses = component_type.__subclasses__()
        if subclasses:
            for subclass in subclasses:
                # Recurse.
                yield from self._iter(subclass, filter_func)

        if component_type in self._components:
            if filter_func is None:
                yield from itertools.chain(*self._components[component_type].values())
            else:
                for component in itertools.chain(*self._components[component_type].values()):
                    if filter_func(component):
                        yield component

    def list_by_name(self, component_type: Type[Component], name: str) -> list[Any]:
        """Return all components that match component_type and name.

        The component_type can be an abstract type.
        """
        return list(self.iter(component_type, filter_func=lambda x: x.name == name))

    def get_by_uuid(self, uuid: UUID) -> Any:
        """Return the component with the input UUID.

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored.
        """
        component = self._components_by_uuid.get(uuid)
        if component is None:
            msg = f"No component with {uuid=} is stored"
            raise ISNotStored(msg)
        return component

    def iter_all(self) -> Iterable[Any]:
        """Return an iterator over all components."""
        return self._components_by_uuid.values()

    def remove(self, component: Component) -> Any:
        """Remove the component from the system and return it.

        Note: users should not call this directly. It should be called through the system
        so that time series is handled.
        """
        if component.has_time_series():
            msg = (
                "remove cannot be called when there is time series data. Call "
                "System.remove_component instead"
            )
            raise ISOperationNotAllowed(msg)

        component_type = type(component)
        # The system method should have already performed the check, but for completeness in case
        # someone calls it directly, check here.
        if (
            component_type not in self._components
            or component.name not in self._components[component_type]
        ):
            msg = f"{component.summary} is not stored"
            raise ISNotStored(msg)

        container = self._components[component_type][component.name]
        for i, comp in enumerate(container):
            if comp.uuid == component.uuid:
                container.pop(i)
                component.system_uuid = None
                if not self._components[component_type][component.name]:
                    self._components[component_type].pop(component.name)
                if not self._components[component_type]:
                    self._components.pop(component_type)
                logger.debug("Removed component {}", component.summary)
                return

        msg = f"Component {component.summary} is not stored"
        raise ISNotStored(msg)

    def copy(
        self,
        component: Component,
        name: str | None = None,
        attach=False,
    ) -> Component:
        """Create a copy of the component. Time series data is excluded."""
        # This uses model_dump and the component constructor because the 'name' field is frozen.
        data = component.model_dump()
        data.pop("time_series_metadata", None)
        for field in ("system_uuid", "uuid"):
            data.pop(field)
        if name is not None:
            data["name"] = name
        new_component = type(component)(**data)  # type: ignore

        logger.info("Copied {} to {}", component.summary, new_component.summary)
        if attach:
            self.add(new_component)

        return new_component

    def change_uuid(self, component: Component) -> None:
        """Change the component UUID."""
        raise NotImplementedError("change_component_uuid")

    def update(
        self,
        component_type: Type[Component],
        update_func: Callable,
        filter_func: Callable | None = None,
    ) -> None:
        """Update multiple components of a given type."""

        for component in self.iter(component_type, filter_func=filter_func):
            update_func(component)
        return

    def _add(self, component: Component, deserialization_in_progress: bool) -> None:
        raise_if_attached(component)
        if not deserialization_in_progress:
            # TODO: Do we want any checks during deserialization? User could change the JSON.
            # We could prevent the user from changing the JSON with a checksum.
            self._check_component_addition(component)
            component.check_component_addition(self._uuid)
        if component.uuid in self._components_by_uuid:
            msg = f"{component.summary} with UUID={component.uuid} is already stored"
            raise ISAlreadyAttached(msg)

        cls = type(component)
        if cls not in self._components:
            self._components[cls] = {}

        name = component.name or component.summary
        if name not in self._components[cls]:
            self._components[cls][name] = []

        self._components[cls][name].append(component)
        self._components_by_uuid[component.uuid] = component
        component.system_uuid = self._uuid
        logger.debug("Added {} to the system", component.summary)

    def _check_component_addition(self, component: Component) -> None:
        """Check all the fields of a component against the setting
        auto_add_composed_components. Recursive."""
        for field in type(component).model_fields:
            val = getattr(component, field)
            if isinstance(val, Component):
                self._handle_composed_component(val)
                # Recurse.
                self._check_component_addition(val)
            if isinstance(val, list) and val and isinstance(val[0], Component):
                for item in val:
                    self._handle_composed_component(item)
                    # Recurse.
                    self._check_component_addition(item)

    def _handle_composed_component(self, component: Component) -> None:
        """Do what's needed for a composed component depending on system settings:
        nothing, add, or raise an exception."""
        if component.system_uuid is not None:
            return

        if self._auto_add_composed_components:
            logger.debug("Auto-add composed component {}", component.summary)
            self._add(component, False)
        else:
            msg = (
                f"Component {component.summary} cannot be added to the system because "
                f"its composed component {component.summary} is not already attached."
            )
            raise ISOperationNotAllowed(msg)
