"""HDF5 time series storage"""

from typing import Type
from uuid import UUID

from infra_sys.time_series_models import SingleTimeSeries, TimeSeriesData
from infra_sys.time_series_storage_base import TimeSeriesStorageBase


class Hdf5TimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in an HDF5 file."""

    def add_time_series(self, time_series: TimeSeriesData) -> None:
        pass

    def get_time_series(
        self, name: str, time_series_type: Type = SingleTimeSeries
    ) -> TimeSeriesData:
        ...

    def get_time_series_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        ...

    def remove_time_series(self, name: str, time_series_type=SingleTimeSeries) -> TimeSeriesData:
        ...

    def remove_time_series_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        ...
