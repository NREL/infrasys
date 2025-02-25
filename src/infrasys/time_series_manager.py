"""Manages time series arrays"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Type

from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys import Component
from infrasys.exceptions import ISInvalidParameter, ISOperationNotAllowed
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.time_series_metadata_store import TimeSeriesMetadataStore
from infrasys.time_series_models import (
    SingleTimeSeries,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase

TIME_SERIES_KWARGS = {
    "time_series_in_memory": False,
    "time_series_read_only": False,
    "time_series_directory": None,
}


def _process_time_series_kwarg(key: str, **kwargs: Any) -> Any:
    return kwargs.get(key, TIME_SERIES_KWARGS[key])


class TimeSeriesManager:
    """Manages time series for a system."""

    def __init__(
        self,
        con: sqlite3.Connection,
        storage: Optional[TimeSeriesStorageBase] = None,
        initialize: bool = True,
        **kwargs,
    ) -> None:
        self._read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        self._storage = storage or self.create_new_storage(**kwargs)
        self._metadata_store = TimeSeriesMetadataStore(con, initialize=initialize)

        # TODO: create parsing mechanism? CSV, CSV + JSON

    @staticmethod
    def create_new_storage(permanent: bool = False, **kwargs):
        base_directory: Path | None = _process_time_series_kwarg("time_series_directory", **kwargs)

        if _process_time_series_kwarg("time_series_in_memory", **kwargs):
            return InMemoryTimeSeriesStorage()
        else:
            if permanent:
                if base_directory is None:
                    msg = "Can't convert to perminant storage without a base directory"
                    raise ISInvalidParameter(msg)
                return ArrowTimeSeriesStorage.create_with_permanent_directory(
                    directory=base_directory
                )

            return ArrowTimeSeriesStorage.create_with_temp_directory(base_directory=base_directory)

    @property
    def metadata_store(self) -> TimeSeriesMetadataStore:
        """Return the time series metadata store."""
        return self._metadata_store

    @property
    def storage(self) -> TimeSeriesStorageBase:
        """Return the time series storage object."""
        return self._storage

    def add(
        self,
        time_series: TimeSeriesData,
        *owners: Component | SupplementalAttribute,
        **user_attributes: Any,
    ) -> None:
        """Store a time series array for one or more components or supplemental attributes.

        Parameters
        ----------
        time_series : TimeSeriesData
            Time series data to store.
        owners : Component | SupplementalAttribute
            Add the time series to all of these components or supplemental attributes.
        user_attributes : Any
            Key/value pairs to store with the time series data. Must be JSON-serializable.

        Raises
        ------
        ISAlreadyAttached
            Raised if the variable name and user attributes match any time series already
            attached to one of the components or supplemental attributes.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        self._handle_read_only()
        if not owners:
            msg = "add_time_series requires at least one component or supplemental attribute"
            raise ISOperationNotAllowed(msg)

        ts_type = type(time_series)
        if not issubclass(ts_type, TimeSeriesData):
            msg = f"The first argument must be an instance of TimeSeriesData: {ts_type}"
            raise ValueError(msg)
        metadata_type = ts_type.get_time_series_metadata_type()
        metadata = metadata_type.from_data(time_series, **user_attributes)

        data_is_stored = self._metadata_store.has_time_series(time_series.uuid)
        # Call this first because it could raise an exception.
        self._metadata_store.add(metadata, *owners)
        if not data_is_stored:
            self._storage.add_time_series(metadata, time_series)

    def get(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes,
    ) -> TimeSeriesData:
        """Return a time series array.

        Raises
        ------
        ISNotStored
            Raised if no time series matches the inputs.
            Raised if the inputs match more than one time series.
        ISOperationNotAllowed
            Raised if the inputs match more than one time series.

        See Also
        --------
        list_time_series
        """
        metadata = self._metadata_store.get_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )
        return self._get_by_metadata(metadata, start_time=start_time, length=length)

    def has_time_series(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        **user_attributes,
    ) -> bool:
        """Return True if the component or supplemental atttribute has time series matching the
        inputs.
        """
        return self._metadata_store.has_time_series_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )

    def list_time_series(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes: Any,
    ) -> list[TimeSeriesData]:
        """Return all time series that match the inputs."""
        metadata = self.list_time_series_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )
        return [self._get_by_metadata(x, start_time=start_time, length=length) for x in metadata]

    def list_time_series_metadata(
        self,
        owner: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        **user_attributes: Any,
    ) -> list[TimeSeriesMetadata]:
        """Return all time series metadata that match the inputs."""
        return self._metadata_store.list_metadata(
            owner,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )

    def remove(
        self,
        *owners: Component | SupplementalAttribute,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        **user_attributes: Any,
    ):
        """Remove all time series arrays matching the inputs.

        Raises
        ------
        ISNotStored
            Raised if no time series match the inputs.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        self._handle_read_only()
        time_series_uuids = self._metadata_store.remove(
            *owners,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )
        missing_uuids = self._metadata_store.list_missing_time_series(time_series_uuids)
        for uuid in missing_uuids:
            self._storage.remove_time_series(uuid)
            logger.info("Removed time series {}.{}", time_series_type, variable_name)

    def copy(
        self,
        dst: Component | SupplementalAttribute,
        src: Component | SupplementalAttribute,
        name_mapping: dict[str, str] | None = None,
    ) -> None:
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst
            Destination component or supplemental attribute
        src
            Source component or supplemental attribute
        name_mapping : dict[str, str]
            Optionally map src names to different dst names.
            If provided and src has a time_series with a name not present in name_mapping, that
            time_series will not copied. If name_mapping is nothing then all time_series will be
            copied with src's names.

        Notes
        -----
        name_mapping is currently not implemented.
        """
        self._handle_read_only()
        raise NotImplementedError

    def _get_by_metadata(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        return self._storage.get_time_series(
            metadata,
            start_time=start_time,
            length=length,
        )

    def serialize(self, dst: Path | str, src: Optional[Path | str] = None) -> None:
        """Serialize the time series data to dst."""
        self._storage.serialize(dst, src)

    @classmethod
    def deserialize(
        cls,
        con: sqlite3.Connection,
        data: dict[str, Any],
        parent_dir: Path | str,
        **kwargs: Any,
    ) -> "TimeSeriesManager":
        """Deserialize the class. Must also call add_reference_counts after deserializing
        components.
        """
        time_series_dir = Path(parent_dir) / data["directory"]

        if _process_time_series_kwarg("time_series_read_only", **kwargs):
            storage = ArrowTimeSeriesStorage.create_with_permanent_directory(time_series_dir)
        else:
            storage = ArrowTimeSeriesStorage.create_with_temp_directory()
            storage.serialize(src=time_series_dir, dst=storage.get_time_series_directory())

        cls_instance = cls(con, storage=storage, initialize=False, **kwargs)

        if _process_time_series_kwarg("time_series_in_memory", **kwargs):
            cls_instance.convert_storage(**kwargs)

        return cls_instance

    def _handle_read_only(self) -> None:
        if self._read_only:
            msg = "Cannot modify time series in read-only mode."
            raise ISOperationNotAllowed(msg)

    def convert_storage(self, **kwargs) -> None:
        """
        Create a new storage instance and copy all time series from the current to new storage
        """
        new_storage = self.create_new_storage(**kwargs)
        for time_series_uuid in self.metadata_store.unique_uuids_by_type(
            SingleTimeSeries.__name__
        ):
            new_storage.add_raw_single_time_series(
                time_series_uuid, self._storage.get_raw_single_time_series(time_series_uuid)
            )

        self._storage = new_storage
        return None
