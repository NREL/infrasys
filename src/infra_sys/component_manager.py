"""Manages components"""

import itertools
import logging
from typing import Callable, Iterable, Type
from uuid import UUID

from infra_sys.component_models import Component, raise_if_attached
from infra_sys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infra_sys.models import make_summary

logger = logging.getLogger(__name__)


class ComponentManager:
    """Manages components"""

    def __init__(self, uuid: UUID):
        self._components: dict[Type, dict[str, list[Component]]] = {}
        self._components_by_uuid: dict[UUID, Component] = {}
        self._uuid = uuid

    def add(self, *args) -> None:
        """Add one or more components to the system.

        Raises
        ------
        ISAlreadyAttached
            Raised if a component is already attached to a system.
        """
        for component in args:
            self._add(component)

    def get(self, component_type: Type, name: str) -> Component:
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

        return components[0]

    def iter(
        self, component_type: Type, filter_func: Callable | None = None
    ) -> Iterable[Component]:
        """Return the components with the passed type and optionally match filter_func.

        IF component_type is an abstract type, all matching subtypes will be returned.
        """
        yield from self._iter(component_type, filter_func)

    def _iter(self, component_type: Type, filter_func: Callable | None) -> Iterable[Component]:
        subclasses = component_type.__subclasses__()
        if subclasses:
            for subclass in subclasses:
                # Recurse.
                yield from self._iter(subclass, filter_func)
        else:
            if component_type in self._components:
                if filter_func is None:
                    yield from itertools.chain(*self._components[component_type].values())
                else:
                    for component in itertools.chain(*self._components[component_type].values()):
                        if filter_func(component):
                            yield component

    def list_by_name(self, component_type: Type, name: str):
        """Return all components that match component_type and name.

        The component_type can be an abstract type.
        """
        return self.iter(component_type, filter_func=lambda x: x.name == name)

    def get_by_uuid(self, uuid: UUID) -> Component:
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

    def iter_all(self) -> Iterable[Component]:
        """Return an iterator over all components."""
        return self._components_by_uuid.values()

    def remove(self, component: Component) -> Component:
        """Remove the component from the system and return it.

        Raises
        ------
        ISNotStored
            Raised if the component is not stored in the system.
        """
        raise NotImplementedError("remove_component")

    def remove_by_name(self, component_type: Type, name: str) -> list[Component]:
        """Remove all components matching the inputs from the system and return them.

        Raises
        ------
        ISNotStored
            Raised if the inputs do not match any components in the system.
        """
        raise NotImplementedError("remove_component_by_name")

    def remove_by_uuid(self, uuid: UUID) -> Component:
        """Remove the components with uuid from the system and return it.

        Raises
        ------
        ISNotStored
            Raised if the UUID is not stored in the system.
        """
        raise NotImplementedError("remove_component_by_uuid")

    def copy(
        self, component: Type, new_name: str, attach_to_system=False, copy_time_series=True
    ) -> Component:
        """Create a copy of the component."""
        raise NotImplementedError("copy")

    def change_uuid(self, component_type: Type, component: Component) -> None:
        """Change the component UUID."""
        raise NotImplementedError("change_component_uuid")

    def update(self, component_type: Type, update_func: Callable, filter_func=None) -> None:
        """Update multiple components of a given type."""

        for component in self.iter(component_type, filter_func=filter_func):
            update_func(component, update_func=update_func)
        return

    def _add(self, component: Component) -> None:
        raise_if_attached(component)
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
        logger.debug("Added %s to the system", component.summary)
