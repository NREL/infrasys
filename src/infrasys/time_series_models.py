"""Defines models for time series arrays."""

import abc
import importlib
import sqlite3
from datetime import datetime, timedelta
from enum import StrEnum
from typing import (
    Any,
    Literal,
    Optional,
    Type,
    TypeAlias,
    Union,
    Sequence,
)
from uuid import UUID

import numpy as np
import pandas as pd
import pint
from numpy.typing import NDArray
from pydantic import (
    Field,
    WithJsonSchema,
    field_serializer,
    field_validator,
    computed_field,
    model_validator,
)
from typing_extensions import Annotated

from infrasys.exceptions import (
    ISConflictingArguments,
)
from infrasys.models import InfraSysBaseModelWithIdentifers, InfraSysBaseModel
from infrasys.normalization import NormalizationModel


TIME_COLUMN = "timestamp"
VALUE_COLUMN = "value"


ISArray: TypeAlias = Sequence | NDArray | pint.Quantity


class TimeSeriesStorageType(StrEnum):
    """Defines the possible storage types for time series."""

    MEMORY = "memory"
    ARROW = "arrow"
    CHRONIFY = "chronify"
    HDF5 = "hdf5"
    PARQUET = "parquet"


class TimeSeriesData(InfraSysBaseModelWithIdentifers, abc.ABC):
    """Base class for all time series models"""

    variable_name: str
    normalization: NormalizationModel = None

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

    data: NDArray | pint.Quantity
    resolution: timedelta
    initial_time: datetime

    @computed_field
    def length(self) -> int:
        """Return the length of the data."""
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SingleTimeSeries):
            raise NotImplementedError
        is_equal = True
        for field in self.model_fields_set:
            if field == "data":
                if not (self.data == other.data).all():
                    is_equal = False
                    break
            else:
                if not getattr(self, field) == getattr(other, field):
                    is_equal = False
                    break
        return is_equal

    @field_validator("data", mode="before")
    @classmethod
    def check_data(cls, data) -> NDArray | pint.Quantity:  # Standarize what object we receive.
        """Check time series data."""
        if len(data) < 2:
            msg = f"SingleTimeSeries length must be at least 2: {len(data)}"
            raise ValueError(msg)

        if isinstance(data, pint.Quantity):
            if not isinstance(data.magnitude, np.ndarray):
                return type(data)(np.array(data.magnitude), units=data.units)
            return data

        if not isinstance(data, np.ndarray):
            return np.array(data)

        return data

    @classmethod
    def from_array(
        cls,
        data: ISArray,
        variable_name: str,
        initial_time: datetime,
        resolution: timedelta,
        normalization: NormalizationModel = None,
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
        if normalization is not None:
            npa = data if isinstance(data, np.ndarray) else np.array(data)
            data = normalization.normalize_array(npa)

        return SingleTimeSeries(
            data=data,  # type: ignore
            variable_name=variable_name,
            initial_time=initial_time,
            resolution=resolution,
            normalization=normalization,
        )

    @classmethod
    def from_time_array(
        cls,
        data: ISArray,
        variable_name: str,
        time_index: Sequence[datetime],
        normalization: NormalizationModel = None,
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
            normalization=normalization,
        )

    def make_timestamps(self) -> NDArray:
        """Return the timestamps as a numpy array."""
        return pd.date_range(
            start=self.initial_time, periods=len(self.data), freq=self.resolution
        ).values

    @staticmethod
    def get_time_series_metadata_type() -> Type:
        return SingleTimeSeriesMetadata

    @property
    def data_array(self) -> NDArray:
        if isinstance(self.data, pint.Quantity):
            return self.data.magnitude
        return self.data


class SingleTimeSeriesScalingFactor(SingleTimeSeries):
    """Defines a time array with a single dimension of floats that are 0-1 scaling factors."""


# TODO:
# read CSV and Parquet and convert each column to a SingleTimeSeries


class QuantityMetadata(InfraSysBaseModel):
    """Contains the metadata needed to de-serialize time series stored within a pint.Quantity."""

    module: str
    quantity_type: Annotated[Type, WithJsonSchema({"type": "string"})]
    units: str

    @field_serializer("quantity_type")
    def serialize_type(self, _):
        return self.quantity_type.__name__

    @model_validator(mode="before")
    @classmethod
    def deserialize_from_strings(cls, values: dict[str, Any]) -> dict[str, Any]:
        if isinstance(values["quantity_type"], str):
            module = importlib.import_module(values["module"])
            return {
                "module": values["module"],
                "quantity_type": getattr(module, values["quantity_type"]),
                "units": values["units"],
            }
        return values


class TimeSeriesMetadata(InfraSysBaseModel, abc.ABC):
    """Defines common metadata for all time series."""

    variable_name: str
    time_series_uuid: UUID
    user_attributes: dict[str, Any] = {}
    quantity_metadata: Optional[QuantityMetadata] = None
    normalization: NormalizationModel = None
    type: Literal["SingleTimeSeries", "SingleTimeSeriesScalingFactor", "NonSequentialTimeSeries"]

    @property
    def label(self) -> str:
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
    initial_time: datetime
    resolution: timedelta
    type: Literal["SingleTimeSeries", "SingleTimeSeriesScalingFactor"]

    @classmethod
    def from_data(cls, time_series: SingleTimeSeries, **user_attributes) -> Any:
        """Construct a SingleTimeSeriesMetadata from a SingleTimeSeries."""
        quantity_metadata = (
            QuantityMetadata(
                module=type(time_series.data).__module__,
                quantity_type=type(time_series.data),
                units=str(time_series.data.units),
            )
            if isinstance(time_series.data, pint.Quantity)
            else None
        )
        return cls(
            variable_name=time_series.variable_name,
            resolution=time_series.resolution,
            initial_time=time_series.initial_time,
            length=time_series.length,  # type: ignore
            time_series_uuid=time_series.uuid,
            user_attributes=user_attributes,
            quantity_metadata=quantity_metadata,
            normalization=time_series.normalization,
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


class NonSequentialTimeSeries(TimeSeriesData):
    """Defines a non-sequential time array with a single dimension of floats."""

    data: NDArray | pint.Quantity
    timestamps: NDArray

    @computed_field
    def length(self) -> int:
        """Return the length of the data."""
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, NonSequentialTimeSeries):
            raise NotImplementedError
        is_equal = True
        for field in self.model_fields_set:
            if field == "data":
                if not (self.data == other.data).all():
                    is_equal = False
                    break
            elif field == "timestamps":
                if not all(t1 == t2 for t1, t2 in zip(self.timestamps, other.timestamps)):
                    is_equal = False
                    break
            else:
                if not getattr(self, field) == getattr(other, field):
                    is_equal = False
                    break
        return is_equal

    @field_validator("data", mode="before")
    @classmethod
    def check_data(cls, data) -> NDArray | pint.Quantity:
        """Check time series data."""
        if len(data) < 2:
            msg = f"NonSequentialTimeSeries length must be at least 2: {len(data)}"
            raise ValueError(msg)

        if isinstance(data, pint.Quantity):
            if not isinstance(data.magnitude, np.ndarray):
                return type(data)(np.array(data.magnitude), units=data.units)
            return data

        if not isinstance(data, np.ndarray):
            return np.array(data)

        return data

    @field_validator("timestamps", mode="before")
    @classmethod
    def check_timestamp(cls, timestamps: Sequence[datetime] | NDArray) -> NDArray:
        """Check non-sequential timestamps."""
        if len(timestamps) < 2:
            msg = f"Time index must have at least 2 timestamps: {len(timestamps)}"
            raise ValueError(msg)

        if len(timestamps) != len(set(timestamps)):
            msg = "Duplicate timestamps found. Timestamps must be unique."
            raise ValueError(msg)

        time_array = np.array(timestamps, dtype="datetime64[ns]")
        if not np.all(np.diff(time_array) > np.timedelta64(0, "s")):
            msg = "Timestamps must be in chronological order."
            raise ValueError(msg)

        if not isinstance(timestamps, np.ndarray):
            return np.array(timestamps)

        return timestamps

    @classmethod
    def from_array(
        cls,
        data: ISArray,
        timestamps: Sequence[datetime] | NDArray,
        variable_name: str,
        normalization: NormalizationModel = None,
    ) -> "NonSequentialTimeSeries":
        """Method of NonSequentialTimeSeries that creates an instance from an array and timestamps.

        Parameters
        ----------
        data
            Sequence that contains the values of the time series
        timestamps
            Sequence that contains the non-sequential timestamps
        variable_name
            Name assigned to the values of the time series (e.g., active_power)
        normalization
            Normalization model to normalize the data

        Returns
        -------
        NonSequentialTimeSeries
        """
        if normalization is not None:
            npa = data if isinstance(data, np.ndarray) else np.array(data)
            data = normalization.normalize_array(npa)

        return NonSequentialTimeSeries(
            data=data,  # type: ignore
            timestamps=timestamps,  # type: ignore
            variable_name=variable_name,
            normalization=normalization,
        )

    @staticmethod
    def get_time_series_metadata_type() -> Type:
        "Get the metadata type of the NonSequentialTimeSeries"
        return NonSequentialTimeSeriesMetadata

    @property
    def data_array(self) -> NDArray:
        "Get the data array NonSequentialTimeSeries"
        if isinstance(self.data, pint.Quantity):
            return self.data.magnitude
        return self.data

    @property
    def timestamps_array(self) -> NDArray:
        "Get the timestamps array NonSequentialTimeSeries"
        return self.timestamps


class NonSequentialTimeSeriesMetadataBase(TimeSeriesMetadata, abc.ABC):
    """Base class for NonSequentialTimeSeries metadata."""

    length: int
    type: Literal["NonSequentialTimeSeries"]

    @classmethod
    def from_data(
        cls, time_series: NonSequentialTimeSeries, **user_attributes
    ) -> "NonSequentialTimeSeriesMetadataBase":
        """Construct a NonSequentialTimeSeriesMetadata from a NonSequentialTimeSeries."""
        quantity_metadata = (
            QuantityMetadata(
                module=type(time_series.data).__module__,
                quantity_type=type(time_series.data),
                units=str(time_series.data.units),
            )
            if isinstance(time_series.data, pint.Quantity)
            else None
        )
        return cls(
            variable_name=time_series.variable_name,
            length=time_series.length,  # type: ignore
            time_series_uuid=time_series.uuid,
            user_attributes=user_attributes,
            quantity_metadata=quantity_metadata,
            normalization=time_series.normalization,
            type=cls.get_time_series_type_str(),  # type: ignore
        )

    @staticmethod
    def get_time_series_data_type() -> Type:
        return NonSequentialTimeSeries


class NonSequentialTimeSeriesMetadata(NonSequentialTimeSeriesMetadataBase):
    """Defines the metadata for a NonSequentialTimeSeries."""

    type: Literal["NonSequentialTimeSeries"] = "NonSequentialTimeSeries"

    @staticmethod
    def get_time_series_type_str() -> str:
        return "NonSequentialTimeSeries"


class TimeSeriesKey(InfraSysBaseModel):
    """Base class for time series keys."""

    variable_name: str
    time_series_type: Type[TimeSeriesData]
    user_attributes: dict[str, Any] = {}


class SingleTimeSeriesKey(TimeSeriesKey):
    """Keys for SingleTimeSeries."""

    length: int
    initial_time: datetime
    resolution: timedelta


class NonSequentialTimeSeriesKey(TimeSeriesKey):
    """Keys for SingleTimeSeries."""

    length: int


class DatabaseConnection(InfraSysBaseModel):
    """Stores connections to the metadata and data databases during transactions."""

    metadata_conn: sqlite3.Connection
    data_conn: Any = None
