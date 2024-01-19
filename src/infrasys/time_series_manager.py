"""Manages time series arrays"""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Type
from uuid import UUID

from loguru import logger

from infrasys.exceptions import ISAlreadyAttached, ISNotStored, ISOperationNotAllowed
from infrasys.component_models import ComponentWithQuantities
from infrasys.models import InfraSysBaseModel
from infrasys.time_series_models import (
    SingleTimeSeries,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from infrasys.parquet_time_series_storage import ParquetTimeSeriesStorage


TIME_SERIES_KWARGS = {
    "time_series_in_memory": True,
    "time_series_read_only": False,
    "time_series_directory": None,
    "enable_time_series_file_compression": True,
}


def _process_time_series_kwarg(key: str, **kwargs):
    return kwargs.get(key, TIME_SERIES_KWARGS[key])


class TimeSeriesMetadataTracker(InfraSysBaseModel):
    """Tracks metadata in memory"""

    metadata: TimeSeriesMetadata
    count: int = 0


class TimeSeriesManager:
    """Manages time series for a system."""

    def __init__(
        self,
        metadata_trackers: None | list[TimeSeriesMetadataTracker] | None = None,
        **kwargs,
    ):
        self._base_directory: Path | None = _process_time_series_kwarg(
            "time_series_directory", **kwargs
        )
        self._read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        if _process_time_series_kwarg("time_series_in_memory", **kwargs):
            self._storage = InMemoryTimeSeriesStorage()
        elif _process_time_series_kwarg("enable_time_series_file_compression", **kwargs):
            self._storage = ParquetTimeSeriesStorage()
        else:
            raise NotImplementedError("Bug: unhandled time series storage options")

        if metadata_trackers:
            self._metadata = {x.metadata.uuid: x for x in metadata_trackers}
        else:
            self._metadata: dict[UUID, TimeSeriesMetadataTracker] = {}

        # TODO: enforce one resolution
        # TODO: create parsing mechanism? CSV, CSV + JSON

    def add(
        self,
        time_series: TimeSeriesData,
        *components: ComponentWithQuantities,
        **user_attributes: Any,
    ) -> None:
        """Store a time series array for one or more components.

        time_series : TimeSeriesData
        components : tuple[ComponentWithQuantities]
        user_attributes : kwargs
            Key/value pairs to store with the time series data.

        Raises
        ------
        ISAlreadyAttached
            Raised if the variable name and user attributes match any time series already
            attached to one of the components.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        if self._read_only:
            raise ISOperationNotAllowed("Cannot store time series in read-only mode")

        ts_type = type(time_series)
        metadata_type = ts_type.get_time_series_metadata_type()
        metadata = metadata_type.from_data(time_series, **user_attributes)
        variable_name = time_series.variable_name

        for component in components:
            if component.has_time_series(
                variable_name=variable_name,
                time_series_type=ts_type,
                **metadata.user_attributes,
            ):
                msg = (
                    f"{component.summary} already has a time series with "
                    f"{ts_type} {variable_name} {user_attributes=}"
                )
                raise ISAlreadyAttached(msg)

        if metadata.uuid not in self._metadata:
            self._metadata[metadata.uuid] = TimeSeriesMetadataTracker(metadata=metadata)
            self._storage.add_time_series(metadata, time_series)

        tracker = self._metadata[metadata.uuid]
        for component in components:
            tracker.count += 1
            component.add_time_series_metadata(metadata)

    def get(
        self,
        component: ComponentWithQuantities,
        variable_name: str,
        time_series_type: Type = SingleTimeSeries,
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
        metadata = component.get_time_series_metadata(
            variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )
        return self._get_by_metadata(metadata, start_time=start_time, length=length)

    def list_time_series(
        self,
        component: ComponentWithQuantities,
        variable_name: str,
        time_series_type: Type = SingleTimeSeries,
        start_time: datetime | None = None,
        length: int | None = None,
        **user_attributes,
    ) -> list[TimeSeriesData]:
        """Return all time series that match the inputs."""
        metadata = component.list_time_series_metadata(
            variable_name,
            time_series_type=time_series_type,
            **user_attributes,
        )
        return [self._get_by_metadata(x, start_time=start_time, length=length) for x in metadata]

    def remove(
        self,
        *components: ComponentWithQuantities,
        variable_name: str | None = None,
        time_series_type=SingleTimeSeries,
        **user_attributes,
    ):
        """Remove all time series arrays matching the inputs.

        Raises
        ------
        ISNotStored
            Raised if no time series match the inputs.
        ISOperationNotAllowed
            Raised if the manager was created in read-only mode.
        """
        if self._read_only:
            raise ISOperationNotAllowed("Cannot remove time series in read-only mode.")

        uuids = defaultdict(lambda: 0)
        for component in components:
            for metadata in component.list_time_series_metadata(
                variable_name, time_series_type=time_series_type, **user_attributes
            ):
                uuids[metadata.uuid] += 1

        for uuid, count in uuids.items():
            if uuid not in self._metadata:
                msg = f"Bug: {uuid=} is not stored in self._metadata"
                raise Exception(msg)
            if count > self._metadata[uuid].count:
                msg = (
                    f"Bug: Removing time series {variable_name=} {time_series_type=} {uuid=}"
                    "will decrease the reference counts below 0."
                )
                raise Exception(msg)

        for component in components:
            component.remove_time_series_metadata(
                variable_name, time_series_type=time_series_type, **user_attributes
            )

        for uuid, count in uuids.items():
            self._metadata[uuid].count -= count
            if self._metadata[uuid].count == 0:
                self._storage.remove_time_series(self._metadata[uuid].metadata)
                self._metadata.pop(uuid)
                logger.info("Removed time series %s.%s", time_series_type, variable_name)

    def copy(
        self,
        dst: ComponentWithQuantities,
        src: ComponentWithQuantities,
        name_mapping: dict[str, str] | None = None,
    ):
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst : ComponentWithQuantities
        src : ComponentWithQuantities
        name_mapping : dict[str, str]
            Optionally map src names to different dst names.
            If provided and src has a time_series with a name not present in name_mapping, that
            time_series will not copied. If name_mapping is nothing then all time_series will be
            copied with src's names.
        """
        if self._read_only:
            raise ISOperationNotAllowed("Cannot copy time series in read-only mode")
        raise NotImplementedError("copy time series")

    def _get_by_metadata(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        if metadata.uuid not in self._metadata:
            msg = f"No time series metadata is stored with {metadata.uuid}"
            raise ISNotStored(msg)
        return self._storage.get_time_series(
            metadata,
            start_time=start_time,
            length=length,
        )

    def serialize_data(self, base_dir) -> list[Path]:
        """Serialize the time series data to one or more files. May be a no-op.

        Returns
        -------
        list[Path]
            Returns all filenames containing the data.
        """
        filename = ""
        return [base_dir / filename]

    def serialize_metadata(self) -> list[dict[str, Any]]:
        """Serialize the time series metadata to a dictionary."""
        return [x.model_dump() for x in self._metadata.values()]

    @classmethod
    def deserialize(
        cls,
        data: dict[str, Any],
        **kwargs,
    ) -> "TimeSeriesManager":
        """Deserialize the class."""
        metadata_trackers = [TimeSeriesMetadataTracker(**x) for x in data["metadata"]]
        # TODO: handle data files
        return cls(metadata_trackers=metadata_trackers, **kwargs)
