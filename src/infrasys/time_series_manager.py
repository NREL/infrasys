"""Manages time series arrays"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Type

from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys import Component
from infrasys.exceptions import ISOperationNotAllowed
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
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

    def __init__(self, storage: Optional[TimeSeriesStorageBase] = None, **kwargs) -> None:
        base_directory: Path | None = _process_time_series_kwarg("time_series_directory", **kwargs)
        self._read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        self._storage = storage or (
            InMemoryTimeSeriesStorage()
            if _process_time_series_kwarg("time_series_in_memory", **kwargs)
            else ArrowTimeSeriesStorage.create_with_temp_directory(base_directory=base_directory)
        )
        self._metadata_store = TimeSeriesMetadataStore()

        # TODO: create parsing mechanism? CSV, CSV + JSON

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
        *components: Component,
        **user_attributes: Any,
    ) -> None:
        """Store a time series array for one or more components.

        Parameters
        ----------
        time_series : TimeSeriesData
            Time series data to store.
        components : Component
            Add the time series to all of these components.
        user_attributes : Any
            Key/value pairs to store with the time series data. Must be JSON-serializable.

        Raises
        ------
        ISAlreadyAttached
            Raised if the variable name and user attributes match any time series already
            attached to one of the components.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        self._handle_read_only()
        if not components:
            msg = "add_time_series requires at least one component"
            raise ISOperationNotAllowed(msg)

        ts_type = type(time_series)
        if not issubclass(ts_type, TimeSeriesData):
            msg = f"The first argument must be an instance of TimeSeriesData: {ts_type}"
            raise ValueError(msg)
        metadata_type = ts_type.get_time_series_metadata_type()
        metadata = metadata_type.from_data(time_series, **user_attributes)

        if not self._metadata_store.has_time_series(time_series.uuid):
            self._storage.add_time_series(metadata, time_series)

        self._metadata_store.add(metadata, *components)

    def get(
        self,
        component: Component,
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
            component,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )
        return self._get_by_metadata(metadata, start_time=start_time, length=length)

    def has_time_series(
        self,
        component: Component,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        **user_attributes,
    ) -> bool:
        """Return True if the component has time series matching the inputs."""
        return self._metadata_store.has_time_series_metadata(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )

    def list_time_series(
        self,
        component: Component,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes: Any,
    ) -> list[TimeSeriesData]:
        """Return all time series that match the inputs."""
        metadata = self.list_time_series_metadata(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )
        return [self._get_by_metadata(x, start_time=start_time, length=length) for x in metadata]

    def list_time_series_metadata(
        self,
        component: Component,
        variable_name: str | None = None,
        time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
        **user_attributes: Any,
    ) -> list[TimeSeriesMetadata]:
        """Return all time series metadata that match the inputs."""
        return self._metadata_store.list_metadata(
            component,
            variable_name=variable_name,
            time_series_type=time_series_type.__name__,
            **user_attributes,
        )

    def remove(
        self,
        *components: Component,
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
            *components,
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
        dst: Component,
        src: Component,
        name_mapping: dict[str, str] | None = None,
    ) -> None:
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst : Component
            Destination component
        src : Component
            Source component
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
        self._metadata_store.backup(dst)

    @classmethod
    def deserialize(
        cls,
        data: dict[str, Any],
        parent_dir: Path | str,
        **kwargs: Any,
    ) -> "TimeSeriesManager":
        """Deserialize the class. Must also call add_reference_counts after deserializing
        components.
        """
        if _process_time_series_kwarg("time_series_in_memory", **kwargs):
            msg = "De-serialization does not support time_series_in_memory"
            raise ISOperationNotAllowed(msg)

        time_series_dir = Path(parent_dir) / data["directory"]

        if _process_time_series_kwarg("time_series_read_only", **kwargs):
            storage = ArrowTimeSeriesStorage.create_with_permanent_directory(time_series_dir)
        else:
            storage = ArrowTimeSeriesStorage.create_with_temp_directory()
            storage.serialize(src=time_series_dir, dst=storage.get_time_series_directory())

        mgr = cls(storage=storage, **kwargs)
        mgr.metadata_store.restore(time_series_dir)
        return mgr

    def _handle_read_only(self) -> None:
        if self._read_only:
            msg = "Cannot modify time series in read-only mode."
            raise ISOperationNotAllowed(msg)
