import logging
from datetime import datetime, timedelta

import pytest
from loguru import logger

from infrasys.location import Location
from infrasys.time_series_models import SingleTimeSeries
from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator, SimpleSubsystem


@pytest.fixture
def simple_system() -> SimpleSystem:
    """Creates a system."""
    system = SimpleSystem(name="test-system")
    geo = Location(x=1.0, y=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    system.add_components(geo, bus, gen, subsystem)
    return system


@pytest.fixture
def simple_system_with_time_series(simple_system) -> SimpleSystem:
    """Creates a system with time series data."""
    variable_name = "active_power"
    length = 8784
    df = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(df, variable_name, start, resolution)
    gen = simple_system.get_component(SimpleGenerator, "test-gen")
    simple_system.add_time_series(ts, gen)
    return simple_system


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
