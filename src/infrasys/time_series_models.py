"""Defines models for time series arrays."""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Iterable, Literal, Optional, Type, Union
from uuid import UUID

import polars as pl
from pydantic import Field, field_validator, model_validator
from typing_extensions import Annotated

from infrasys.exceptions import ISConflictingArguments
from infrasys.models import InfraSysBaseModelWithIdentifers

TIME_COLUMN = "timestamp"
VALUE_COLUMN = "value"


class TimeSeriesStorageType(str, Enum):
    """Defines the possible storage types for time series."""

    HDF5 = "hdf5"
    IN_MEMORY = "in_memory"
    PARQUET = "parquet"


class TimeSeriesData(InfraSysBaseModelWithIdentifers):
    """Base class for all time series models"""

    variable_name: str

    @property
    def summary(self) -> str:
        """Return the variable_name of the time series array with its type."""
        return f"{self.__class__.__name__}.{self.variable_name}"


class SingleTimeSeries(TimeSeriesData):
    """Defines a time array with a single dimension of floats."""

    resolution: Optional[timedelta] = None
    initial_time: Optional[datetime] = None
    length: Optional[int] = None
    data: pl.DataFrame

    @field_validator("data")
    @classmethod
    def check_data(cls, df) -> pl.DataFrame:
        """Check time series data."""
        if len(df) < 3:
            msg = f"SingleTimeSeries length must be at least 2: {len(df)}"
            raise ValueError(msg)

        if TIME_COLUMN not in df.columns:
            msg = f"SingleTimeSeries dataframe must have the time column {TIME_COLUMN}"
            raise ValueError(msg)

        if VALUE_COLUMN not in df.columns:
            msg = f"SingleTimeSeries dataframe must have the value column {VALUE_COLUMN}"
            raise ValueError(msg)

        return df

    @model_validator(mode="after")
    def assign_values(self) -> "SingleTimeSeries":
        """Assign parameters by inspecting data."""
        actual_res = self.data[TIME_COLUMN][1] - self.data[TIME_COLUMN][0]
        actual_len = len(self.data)
        actual_it = self.data[TIME_COLUMN][0]

        if self.resolution is None:
            self.resolution = actual_res
        elif self.resolution != actual_res:
            msg = f"resolution={self.resolution} does not match data resolution {actual_res}"
            raise ValueError(msg)

        if self.length is None:
            self.length = actual_len
        elif self.length != actual_len:
            msg = f"length={self.length} does not match data length {actual_len}"
            raise ValueError(msg)

        if self.initial_time is None:
            self.initial_time = actual_it
        elif self.initial_time != actual_it:
            msg = f"initial_time={self.initial_time} does not match data initial_time {actual_it}"
            raise ValueError(msg)

        return self

    @classmethod
    def from_array(
        cls, data: Iterable, variable_name: str, initial_time: datetime, resolution: timedelta
    ) -> "SingleTimeSeries":
        """Create a SingleTimeSeries from an iterable of data. Length is inferred from data."""
        length = len(data)
        end_time = initial_time + (length - 1) * resolution
        df = pl.DataFrame(
            {
                TIME_COLUMN: pl.datetime_range(
                    initial_time, end_time, interval=resolution, eager=True
                ),
                VALUE_COLUMN: data,
            }
        )
        return SingleTimeSeries(variable_name=variable_name, data=df)

    @classmethod
    def from_dataframe(
        cls,
        df: pl.DataFrame,
        variable_name: str,
        time_column=TIME_COLUMN,
        value_column=VALUE_COLUMN,
    ) -> "SingleTimeSeries":
        """Create a SingleTimeSeries from a DataFrame with a time column."""
        data = df.select(
            pl.col(time_column).alias(TIME_COLUMN), pl.col(value_column).alias(VALUE_COLUMN)
        )
        return SingleTimeSeries(variable_name=variable_name, data=data)

    @staticmethod
    def get_time_series_metadata_type() -> Type:
        """Return the metadata type associated with this time series type."""
        return SingleTimeSeriesMetadata


class SingleTimeSeriesScalingFactor(SingleTimeSeries):
    """Defines a time array with a single dimension of floats that are 0-1 scaling factors."""


# TODO:
# read CSV and Parquet and convert each column to a SingleTimeSeries


class TimeSeriesMetadata(InfraSysBaseModelWithIdentifers):
    """Defines common metadata for all time series."""

    variable_name: str
    initial_time: datetime
    resolution: timedelta
    time_series_uuid: UUID
    user_attributes: dict[str, Any] = {}
    type: Literal["SingleTimeSeries", "SingleTimeSeriesScalingFactor"]

    @property
    def summary(self) -> str:
        """Return the variable_name of the time series array with its type."""
        return f"{self.type}.{self.variable_name}"


class SingleTimeSeriesMetadata(TimeSeriesMetadata):
    """Defines the metadata for a SingleTimeSeries."""

    length: int
    type: Literal["SingleTimeSeries"] = "SingleTimeSeries"

    @classmethod
    def from_data(
        cls, time_series: SingleTimeSeries, **user_attributes
    ) -> "SingleTimeSeriesMetadata":
        """Construct a SingleTimeSeriesMetadata from a SingleTimeSeries."""
        return cls(
            variable_name=time_series.variable_name,
            resolution=time_series.resolution,
            initial_time=time_series.initial_time,
            length=time_series.length,
            time_series_uuid=time_series.uuid,
            user_attributes=user_attributes,
        )

    def get_range(
        self, start_time: datetime | None = None, length: int | None = None
    ) -> tuple[int, int]:
        """Return the range to be used to index into the dataframe."""
        if start_time is None and length is None:
            return (0, self.length)

        if start_time is None:
            index = 0
        else:
            if start_time < self.initial_time:
                msg = "{start_time=} is less than {self.initial_time=}"
                raise ISConflictingArguments(msg)
            if start_time >= self.initial_time + self.length * self.resolution:
                msg = f"{start_time=} is too large: {self=}"
                raise ISConflictingArguments(msg)
            diff = start_time - self.initial_time
            if (diff % self.resolution).total_seconds() != 0.0:
                msg = (
                    f"{start_time=} conflicts with initial_time={self.initial_time} and "
                    f"resolution={self.resolution}"
                )
                raise ISConflictingArguments(msg)
            index = int(diff / self.resolution)
        if length is None:
            length = self.length - index

        if index + length > self.length:
            msg = f"{start_time=} {length=} conflicts with {self=}"
            raise ISConflictingArguments(msg)

        return (index, length)

    @staticmethod
    def get_time_series_data_type() -> Type:
        """Return the data type associated with this metadata type."""
        return SingleTimeSeries


class SingleTimeSeriesScalingFactorMetadata(SingleTimeSeriesMetadata):
    """Defines the metadata for a SingleTimeSeriesScalingFactor."""

    type: Literal["SingleTimeSeriesScalingFactor"] = "SingleTimeSeriesScalingFactor"


# This needs to be a Union if we add other time series types.
TimeSeriesMetadataUnion = Annotated[
    Union[SingleTimeSeriesMetadata, SingleTimeSeriesScalingFactorMetadata],
    Field(discriminator="type"),
]
