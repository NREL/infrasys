"""Defines models that can be used for testing the package."""

from uuid import UUID

from infra_sys.exceptions import ISOperationNotAllowed
from infra_sys.component_models import (
    ComponentWithQuantities,
)
from infra_sys.geography_coordinates import GeographyCoordinates
from infra_sys.system import System


class SimpleBus(ComponentWithQuantities):
    """Represents a bus."""

    voltage: float
    coordinates: GeographyCoordinates | None = None

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
            coordinates=GeographyCoordinates(latitude=0.0, longitude=0.0),
        )


class SimpleGenerator(ComponentWithQuantities):
    """Represents a generator."""

    available: bool
    bus: SimpleBus
    rating: float

    @classmethod
    def example(cls) -> "SimpleGenerator":
        return SimpleGenerator(
            name="simple-gen",
            available=True,
            bus=SimpleBus.example(),
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
