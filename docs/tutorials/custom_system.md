# Custom System
This tutorial describes how to create a custom system in a parent package.

1. Define the system. This example defines some custom attributes to illustrate serialization and
de-serialization behaviors.

```python
from infrasys import System

class System(System):
    """Custom System"""

    def __init__(self, my_attribute=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_format_version = "1.1.0"
        self.my_attribute = my_attribute

    def serialize_system_attributes(self) -> dict[str, Any]:
        return {"my_attribute": self.my_attribute}

    def deserialize_system_attributes(self, data: dict[str, Any]) -> None:
        self.my_attribute = data["my_attribute"]

    def handle_data_format_upgrade(self, data: dict[str, Any], from_version, to_version)) -> None:
        ...
```

**Notes**:

- The system's custom attribute `my_attribute` will be serialized and de-serialized automatically.
- `infrasys` will call handle_data_format_upgrade during de-serialization so that this package
can handle format changes that might occur in the future.

2. Define some component classes.

```python
from infrasys.component_models import  ComponentWithQuantities
from infrasys.location import Location

class Bus(ComponentWithQuantities):
    """Represents a bus."""

    voltage: float
    coordinates: Location | None = None

    def check_component_addition(self, system_uuid: UUID):
        if self.coordinates is not None and not self.coordinates.is_attached(
            system_uuid=system_uuid
        ):
            msg = f"{self.summary} has coordinates that are not attached to the system"
            raise ISOperationNotAllowed(msg)

    @classmethod
    def example(cls) -> "Bus":
        return Bus(
            name="my-bus",
            voltage=1.1,
            coordinates=Location(x=0.0, y=0.0),
        )

class Generator(ComponentWithQuantities):
    """Represents a generator."""

    available: bool
    bus: Bus
    active_power: float
    rating: float

    @classmethod
    def example(cls) -> "Generator":
        return Generator(
            name="simple-gen",
            available=True,
            bus=Bus.example(),
            active_power=1.0,
            rating=0.0,
        )
```

**Notes**:

- Each component defines the `example` method. This is highly recommended so that users can see
what a component might look like in the REPL.

- The `Bus` class implements a custom check when it is added to the system. It raises an exception
if its `Location` object is not already attached to the system. The same could be done for
generators and buses.
