"""HDF5 time series storage"""

from uuid import UUID

from infra_sys.time_series_models import TimeSeriesData
from infra_sys.time_series_storage_base import TimeSeriesStorageBase


class Hdf5TimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in an HDF5 file."""

    def add_time_series(self, time_series: TimeSeriesData) -> None:
        pass

    def get_time_series(self, uuid: UUID) -> TimeSeriesData:
        ...

    def remove_time_series(self, uuid: UUID) -> TimeSeriesData:
        ...
