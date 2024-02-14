"""Defines models for time series arrays."""

import abc
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Literal, Type, TypeAlias, Union, Sequence
from uuid import UUID

import numpy as np
import pyarrow as pa
from pydantic import Field, field_validator, computed_field
from typing_extensions import Annotated

from infrasys.exceptions import ISConflictingArguments
from infrasys.models import InfraSysBaseModelWithIdentifers

TIME_COLUMN = "timestamp"
VALUE_COLUMN = "value"


ISArray: TypeAlias = Sequence | pa.Array | np.ndarray


class TimeSeriesStorageType(str, Enum):
    """Defines the possible storage types for time series."""

    HDF5 = "hdf5"
    IN_MEMORY = "in_memory"
    FILE = "arrow"
    PARQUET = "parquet"


class TimeSeriesData(InfraSysBaseModelWithIdentifers, abc.ABC):
    """Base class for all time series models"""

    variable_name: str

    @property
    def summary(self) -> str:
        """Return the variable_name of the time series array with its type."""
        return f"{self.__class__.__name__}.{self.variable_name}"

    @staticmethod
    @abc.abstractmethod
    def get_time_series_metadata_type() -> Type:
        """Return the metadata type associated with this time series type."""


class SingleTimeSeries(TimeSeriesData):
    """Defines a time array with a single dimension of floats."""

    data: ISArray
    resolution: timedelta
    initial_time: datetime

    @computed_field
    def length(self) -> int:
        """Return the length of the data."""
        return len(self.data)

    @field_validator("data")
    @classmethod
    def check_data(cls, data) -> pa.Array:  # Standarize what object we receive.
        """Check time series data."""
        if len(data) < 2:
            msg = f"SingleTimeSeries length must be at least 2: {len(data)}"
            raise ValueError(msg)

        if not isinstance(data, pa.Array):
            data = pa.array(data)

        return data  # type: ignore

    @classmethod
    def from_array(
        cls,
        data: ISArray,
        variable_name: str,
        initial_time: datetime,
        resolution: timedelta,
    ) -> "SingleTimeSeries":
        """Method of SingleTimeSeries that creates an instance from a sequence.

        Parameters
        ----------
        data
            Sequence that contains the values of the time series
        initial_time
            Start time for the time series (e.g., datetime(2020,1,1))
        resolution
            Resolution of the time series (e.g., 30min, 1hr)
        variable_name
            Name assigned to the values of the time series (e.g., active_power)

        Returns
        -------
        SingleTimeSeries

        See Also
        --------
        from_time_array:  Time index implementation

        Note
        ----
        - Length of the sequence is inferred from the data.
        """
        return SingleTimeSeries(
            data=data,
            variable_name=variable_name,
            initial_time=initial_time,
            resolution=resolution,
        )

    @classmethod
    def from_time_array(
        cls,
        data: ISArray,
        variable_name: str,
        time_index: Sequence[datetime],
    ) -> "SingleTimeSeries":
        """Create SingleTimeSeries using time_index provided.

        Parameters
        ----------
        data
            Sequence that contains the values of the time series
        variable_name
            Name assigned to the values of the time series (e.g., active_power)
        time_index
            Sequence that contains the index of the time series

        Returns
        -------
        SingleTimeSeries

        See Also
        --------
        from_array: Base implementation

        Note
        ----
        The current implementation only uses the time_index to infer the initial time and resolution.

        """
        # Infer initial time from the time_index.
        initial_time = time_index[0]

        # This does not cover changes mult-resolution time index.
        resolution = time_index[1] - time_index[0]

        return SingleTimeSeries.from_array(
            data,
            variable_name,
            initial_time,
            resolution,
        )

    @staticmethod
    def get_time_series_metadata_type() -> Type:
        return SingleTimeSeriesMetadata


class SingleTimeSeriesScalingFactor(SingleTimeSeries):
    """Defines a time array with a single dimension of floats that are 0-1 scaling factors."""


# TODO:
# read CSV and Parquet and convert each column to a SingleTimeSeries


class TimeSeriesMetadata(InfraSysBaseModelWithIdentifers, abc.ABC):
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

    @staticmethod
    @abc.abstractmethod
    def get_time_series_data_type() -> Type:
        """Return the data type associated with this metadata type."""
        pass

    @staticmethod
    @abc.abstractmethod
    def get_time_series_type_str() -> str:
        """Return the time series type as a string."""


class SingleTimeSeriesMetadataBase(TimeSeriesMetadata, abc.ABC):
    """Base class for SingleTimeSeries metadata."""

    length: int
    type: Literal["SingleTimeSeries", "SingleTimeSeriesScalingFactor"]

    @classmethod
    def from_data(cls, time_series: SingleTimeSeries, **user_attributes) -> Any:
        """Construct a SingleTimeSeriesMetadata from a SingleTimeSeries."""
        return cls(
            variable_name=time_series.variable_name,
            resolution=time_series.resolution,
            initial_time=time_series.initial_time,
            length=time_series.length,
            time_series_uuid=time_series.uuid,
            user_attributes=user_attributes,
            type=cls.get_time_series_type_str(),  # type: ignore
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
        return SingleTimeSeries


class SingleTimeSeriesMetadata(SingleTimeSeriesMetadataBase):
    """Defines the metadata for a SingleTimeSeries."""

    type: Literal["SingleTimeSeries"] = "SingleTimeSeries"

    @staticmethod
    def get_time_series_type_str() -> str:
        return "SingleTimeSeries"


class SingleTimeSeriesScalingFactorMetadata(SingleTimeSeriesMetadataBase):
    """Defines the metadata for a SingleTimeSeriesScalingFactor."""

    type: Literal["SingleTimeSeriesScalingFactor"] = "SingleTimeSeriesScalingFactor"

    @staticmethod
    def get_time_series_type_str() -> str:
        return "SingleTimeSeriesScalingFactor"


TimeSeriesMetadataUnion = Annotated[
    Union[SingleTimeSeriesMetadata, SingleTimeSeriesScalingFactorMetadata],
    Field(discriminator="type"),
]
