import importlib.metadata as metadata

from loguru import logger

logger.disable("infrasys")

__version__ = metadata.metadata("infrasys")["Version"]

TIME_SERIES_ASSOCIATIONS_TABLE = "time_series_associations"
TIME_SERIES_METADATA_TABLE = "time_series_metadata"
KEY_VALUE_STORE_TABLE = "key_value_store"

from .base_quantity import BaseQuantity
from .component import Component
from .location import GeographicInfo, Location
from .normalization import NormalizationModel
from .supplemental_attribute import SupplementalAttribute
from .system import System
from .time_series_models import (
    NonSequentialTimeSeries,
    SingleTimeSeries,
    SingleTimeSeriesKey,
    TimeSeriesKey,
    TimeSeriesStorageType,
)

__all__ = (
    "BaseQuantity",
    "Component",
    "GeographicInfo",
    "Location",
    "NormalizationModel",
    "SingleTimeSeries",
    "NonSequentialTimeSeries",
    "SingleTimeSeriesKey",
    "SupplementalAttribute",
    "System",
    "TimeSeriesKey",
    "TimeSeriesStorageType",
)
