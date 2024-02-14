import logging

import pytest
from loguru import logger

from infrasys.location import Location
from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator, SimpleSubsystem


@pytest.fixture
def simple_system():
    """Creates a system."""
    system = SimpleSystem(name="test-system")
    geo = Location(x=1.0, y=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    system.components.add(geo, bus, gen, subsystem)
    return system


@pytest.fixture(autouse=True)
def propagate_logs():
    """Enable logging for the package"""

    class PropagateHandler(logging.Handler):
        def emit(self, record):
            if logging.getLogger(record.name).isEnabledFor(record.levelno):
                logging.getLogger(record.name).handle(record)

    logger.remove()
    logger.enable("infrasys")
    logger.add(PropagateHandler(), format="{message}")
    yield
