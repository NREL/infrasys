import importlib.metadata as metadata

from loguru import logger

logger.disable("infrasys")

__version__ = metadata.metadata("infrasys")["Version"]
