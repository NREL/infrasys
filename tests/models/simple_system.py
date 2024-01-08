"""Defines models that can be used for testing the package."""

from typing import Any
from uuid import UUID

from infra_sys.exceptions import ISOperationNotAllowed
from infra_sys.component_models import (
    ComponentWithQuantities,
)
from infra_sys.location import Location
from infra_sys.system import System


class SimpleBus(ComponentWithQuantities):
    """Represents a bus."""

    voltage: float
    coordinates: Location | None = None

    def check_component_addition(self, system_uuid: UUID):
        if self.coordinates is not None and not self.coordinates.is_attached(
            system_uuid=system_uuid
        ):
            # Other packages might want to auto-add in the System class.
            msg = f"{self.summary} has coordinates that are not attached to the system"
            raise ISOperationNotAllowed(msg)

    @classmethod
    def example(cls) -> "SimpleBus":
        return SimpleBus(
            name="simple-bus",
            voltage=1.1,
            coordinates=Location(x=0.0, y=0.0),
        )


class GeneratorBase(ComponentWithQuantities):
    """Base class for generators"""

    available: bool
    bus: SimpleBus


class SimpleGenerator(GeneratorBase):
    """Represents a generator."""

    active_power: float
    rating: float

    @classmethod
    def example(cls) -> "SimpleGenerator":
        return SimpleGenerator(
            name="simple-gen",
            available=True,
            bus=SimpleBus.example(),
            active_power=1.0,
            rating=0.0,
        )


class RenewableGenerator(GeneratorBase):
    """Represents a generator."""

    active_power: float
    rating: float

    @classmethod
    def example(cls) -> "RenewableGenerator":
        return RenewableGenerator(
            name="renewable-gen",
            available=True,
            bus=SimpleBus.example(),
            active_power=1.0,
            rating=0.0,
        )


class SimpleSubsystem(ComponentWithQuantities):
    """Represents a subsystem."""

    generators: list[SimpleGenerator]

    @classmethod
    def example(cls) -> "SimpleSubsystem":
        return SimpleSubsystem(
            name="simple-subsystem",
            generators=[SimpleGenerator.example()],
        )


class SimpleSystem(System):
    """System used for testing"""

    def __init__(self, my_attr=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_format_version = "1.0.3"
        self.my_attr = my_attr

    def serialize_system_attributes(self) -> dict[str, Any]:
        return {"my_attr": self.my_attr}

    def deserialize_system_attributes(self, data: dict[str, Any]) -> None:
        self.my_attr = data["my_attr"]
