"""Defines models for time series arrays."""

from datetime import datetime, timedelta
from enum import Enum
from typing import Literal, Type

import polars as pl
from pydantic import Field
from typing_extensions import Annotated

from infra_sys.models import InfraSysBaseModelWithIdentifers


class TimeSeriesStorageType(str, Enum):
    """Defines the possible storage types for time series."""

    HDF5 = "hdf5"
    IN_MEMORY = "in_memory"
    PARQUET = "parquet"


class TimeSeriesData(InfraSysBaseModelWithIdentifers):
    """Base class for all time series models"""

    name: str

    @property
    def summary(self) -> str:
        """Return the name of the time series array with its type."""
        # TODO: Does this include package name?
        return f"{self.__class__.__name__}.{self.name}"


class TimeSeriesMetadata(InfraSysBaseModelWithIdentifers):
    """Defines common metadata for all time series."""

    name: str
    resolution: timedelta


class SingleTimeSeriesMetadata(TimeSeriesMetadata):
    """Defines the metadata for a SingleTimeSeries."""

    initial_time: datetime
    length: int
    type: Literal["SingleTimeSeries"] = "SingleTimeSeries"


class SingleTimeSeries(SingleTimeSeriesMetadata):
    """Defines a time array with a single dimension of floats."""

    data: pl.DataFrame


# This needs to be a Union if we add other time series types.
TimeSeriesMetadataUnion = Annotated[SingleTimeSeriesMetadata, Field(discriminator="type")]


def get_time_series_type_from_metadata(metadata: TimeSeriesMetadataUnion) -> Type:
    """Return the time series type from the metadata."""
    match metadata.type:
        case "SingleTimeSeries":
            return SingleTimeSeries
        case _:
            raise NotImplementedError(metadata.type)
