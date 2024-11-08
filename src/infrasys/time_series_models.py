"""Defines models for time series arrays."""

import abc
import importlib
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any,
    Literal,
    Optional,
    Self,
    Type,
    TypeAlias,
    Union,
    Sequence,
)
from uuid import UUID

import numpy as np
import pyarrow as pa
import pint
from pydantic import (
    Field,
    WithJsonSchema,
    field_serializer,
    field_validator,
    computed_field,
    model_validator,
)
from typing_extensions import Annotated

from infrasys.base_quantity import BaseQuantity
from infrasys.exceptions import (
    ISConflictingArguments,
    InconsistentTimeseriesAggregation,
)
from infrasys.models import InfraSysBaseModelWithIdentifers, InfraSysBaseModel
from infrasys.normalization import NormalizationModel


TIME_COLUMN = "timestamp"
VALUE_COLUMN = "value"


ISArray: TypeAlias = Sequence | pa.Array | np.ndarray | pint.Quantity


class TimeSeriesStorageType(str, Enum):
    """Defines the possible storage types for time series."""

    HDF5 = "hdf5"
    IN_MEMORY = "in_memory"
    FILE = "arrow"
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

    data: pa.Array | pint.Quantity
    resolution: timedelta
    initial_time: datetime

    @computed_field
    def length(self) -> int:
        """Return the length of the data."""
        return len(self.data)

    @field_validator("data", mode="before")
    @classmethod
    def check_data(
        cls, data
    ) -> pa.Array | pa.ChunkedArray | pint.Quantity:  # Standarize what object we receive.
        """Check time series data."""
        if len(data) < 2:
            msg = f"SingleTimeSeries length must be at least 2: {len(data)}"
            raise ValueError(msg)

        if isinstance(data, pint.Quantity):
            return data

        if not isinstance(data, pa.Array):
            return pa.array(data)

        return data

    @classmethod
    def aggregate(cls, ts_data: list[Self]) -> Self:
        """Method to aggregate list of SingleTimeSeries data.

        Parameters
        ----------
        ts_data
            list of SingleTimeSeries data

        Returns
        -------
        SingleTimeSeries

        Raises
        ------
        InconsistentTimeseriesAggregation
            Raised if incompatible timeseries data are passed.
        """

        # Extract unique properties from ts_data
        unique_props = {
            "length": {data.length for data in ts_data},
            "resolution": {data.resolution for data in ts_data},
            "start_time": {data.initial_time for data in ts_data},
            "variable": {data.variable_name for data in ts_data},
            "data_type": {type(data.data) for data in ts_data},
        }

        # Validate uniformity across properties
        if any(len(prop) != 1 for prop in unique_props.values()):
            inconsistent_props = {k: v for k, v in unique_props.items() if len(v) > 1}
            msg = f"Inconsistent timeseries data: {inconsistent_props}"
            raise InconsistentTimeseriesAggregation(msg)

        # Aggregate data
        is_quantity = issubclass(next(iter(unique_props["data_type"])), BaseQuantity)
        magnitude_type = (
            type(ts_data[0].data.magnitude)
            if is_quantity
            else next(iter(unique_props["data_type"]))
        )

        # Aggregate data based on magnitude type
        if issubclass(magnitude_type, pa.Array):
            new_data = sum(
                [
                    data.data.to_numpy() * (data.data.units if is_quantity else 1)
                    for data in ts_data
                ]
            )
        elif issubclass(magnitude_type, np.ndarray):
            new_data = sum([data.data for data in ts_data])
        elif issubclass(magnitude_type, list) and not is_quantity:
            new_data = sum([np.array(data) for data in ts_data])
        else:
            msg = f"Unsupported data type for aggregation: {magnitude_type}"
            raise TypeError(msg)

        # Return new SingleTimeSeries instance
        return SingleTimeSeries(
            data=new_data,
            variable_name=unique_props["variable"].pop(),
            initial_time=unique_props["start_time"].pop(),
            resolution=unique_props["resolution"].pop(),
            normalization=None,
        )

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

    @staticmethod
    def get_time_series_metadata_type() -> Type:
        return SingleTimeSeriesMetadata


class SingleTimeSeriesScalingFactor(SingleTimeSeries):
    """Defines a time array with a single dimension of floats that are 0-1 scaling factors."""


# TODO:
# read CSV and Parquet and convert each column to a SingleTimeSeries


class QuantityMetadata(InfraSysBaseModel):
    """Contains the metadata needed to de-serialize time series stored within a BaseQuantity."""

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
    initial_time: datetime
    resolution: timedelta
    time_series_uuid: UUID
    user_attributes: dict[str, Any] = {}
    quantity_metadata: Optional[QuantityMetadata] = None
    normalization: NormalizationModel = None
    type: Literal["SingleTimeSeries", "SingleTimeSeriesScalingFactor"]

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
            if isinstance(time_series.data, BaseQuantity)
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
