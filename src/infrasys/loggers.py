"""Contains logging configuration data."""
import sys

# Logger printing formats
DEFAULT_FORMAT = "<level>{level}</level>: {message}"
DEBUG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <7}</level> | "
    "<cyan>{name}:{line}</cyan> | "
    "{message}"
)


def setup_logging(
    filename=None,
    level="DEBUG",
) -> None:
    """Configures logging to file and console.

    Parameters
    ----------
    filename : str | None
        log filename
    level : str, optional
        change defualt level of logging.
    verbose :  bool
        returns additional logging information.
    """
    from loguru import logger

    logger.remove()
    logger.enable("infrasys")
    # logger.enable("resource_monitor")
    logger.add(sys.stderr, level=level, format=DEFAULT_FORMAT)
    if filename:
        logger.add(filename, level=level)


if __name__ == "__main__":
    from infrasys import Component, System

    setup_logging(level="INFO")
    system = System()

    component_1 = Component(name="TestComponent")
    component_2 = Component(name="TestComponent2")

    # Adding components
    system.add_component(component_1)
    system.add_component(component_2)
