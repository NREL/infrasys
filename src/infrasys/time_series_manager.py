"""Manages time series arrays"""

import atexit
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from shutil import copytree
from tempfile import mkdtemp
from typing import Any, Type

from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys.component_models import ComponentWithQuantities
from infrasys.exceptions import ISAlreadyAttached, ISOperationNotAllowed
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from infrasys.models import InfraSysBaseModel
from infrasys.time_series_models import (
    SingleTimeSeries,
    TimeSeriesData,
    TimeSeriesMetadata,
)

TIME_SERIES_KWARGS = {
    "time_series_in_memory": False,
    "time_series_read_only": False,
    "time_series_directory": None,
}


def _process_time_series_kwarg(key: str, **kwargs):
    return kwargs.get(key, TIME_SERIES_KWARGS[key])


class TimeSeriesMetadataTracker(InfraSysBaseModel):
    """Tracks metadata in memory"""

    metadata: TimeSeriesMetadata
    count: int = 0


class TimeSeriesManager:
    """Manages time series for a system."""

    def __init__(self, **kwargs):
        base_directory: Path | None = _process_time_series_kwarg("time_series_directory", **kwargs)

        self._ts_directory = Path(mkdtemp(dir=base_directory))
        logger.debug("Creating tmp folder at {}", self._ts_directory)
        atexit.register(clean_tmp_folder, self._ts_directory)

        self._read_only = _process_time_series_kwarg("time_series_read_only", **kwargs)
        if _process_time_series_kwarg("time_series_in_memory", **kwargs):
            self._storage = InMemoryTimeSeriesStorage()
        else:
            self._storage = ArrowTimeSeriesStorage(self._ts_directory)

        # This tracks the number of references to each time series array across components.
        # When an array is removed and no references remain, it can be deleted.
        # This is only tracked in memory and has to be rebuilt during deserialization.
        self._ref_counts = defaultdict(lambda: 0)

        # TODO: enforce one resolution
        # TODO: create parsing mechanism? CSV, CSV + JSON

    def add(
        self,
        time_series: TimeSeriesData,
        *components: ComponentWithQuantities,
        **user_attributes: Any,
    ) -> None:
        """Store a time series array for one or more components.

        Parameters
        ----------
        time_series : TimeSeriesData
            Time series data to store.
        components : ComponentWithQuantities
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

        if time_series.uuid not in self._ref_counts:
            self._storage.add_time_series(metadata, time_series)

        for component in components:
            self._ref_counts[time_series.uuid] += 1
            component.add_time_series_metadata(metadata)

    def add_reference_counts(self, component: ComponentWithQuantities):
        """Must be called for each component after deserialization in order to rebuild the
        reference counts for each time array.
        """
        for metadata in component.list_time_series_metadata():
            self._ref_counts[metadata.time_series_uuid] += 1

    def get(
        self,
        component: ComponentWithQuantities,
        variable_name: str | None = None,
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
        variable_name: str | None = None,
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
        self._handle_read_only()
        uuids = defaultdict(lambda: 0)
        all_metadata = []
        for component in components:
            for metadata in component.list_time_series_metadata(
                variable_name, time_series_type=time_series_type, **user_attributes
            ):
                uuids[metadata.time_series_uuid] += 1
                all_metadata.append(metadata)

        for uuid, count in uuids.items():
            if uuid not in self._ref_counts:
                msg = f"Bug: {uuid=} is not stored in self._ref_counts"
                raise Exception(msg)
            if count > self._ref_counts[uuid]:
                msg = (
                    f"Bug: Removing time series {variable_name=} {time_series_type=} {uuid=}"
                    "will decrease the reference counts below 0."
                )
                raise Exception(msg)

        count_removed = 0
        for component in components:
            removed = component.remove_time_series_metadata(
                variable_name, time_series_type=time_series_type, **user_attributes
            )
            count_removed += len(removed)

        if count_removed != len(all_metadata):
            msg = f"Bug: {count_removed=} {len(all_metadata)=}"
            raise Exception(msg)

        for metadata in all_metadata:
            self._ref_counts[metadata.time_series_uuid] -= 1
            if self._ref_counts[metadata.time_series_uuid] <= 0:
                self._storage.remove_time_series(metadata)
                self._ref_counts.pop(metadata.time_series_uuid)
                logger.info("Removed time series {}.{}", time_series_type, variable_name)

    def copy(
        self,
        dst: ComponentWithQuantities,
        src: ComponentWithQuantities,
        name_mapping: dict[str, str] | None = None,
    ) -> None:
        """Copy all time series from src to dst.

        Parameters
        ----------
        dst : ComponentWithQuantities
            Destination component
        src : ComponentWithQuantities
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

    def serialize(self, dst, src=None):
        """Serialize the time series data to base_dir."""
        # From the shutil documentation: the copying operation will continue if
        # it encounters existing directories, and files within the dst tree
        # will be overwritten by corresponding files from the src tree.
        if src is None:
            src = self._ts_directory
        copytree(src, dst, dirs_exist_ok=True)
        logger.info("Copied time series data to {}", dst)

    @classmethod
    def deserialize(
        cls,
        data: dict[str, Any],
        parent_dir: Path | str,
        **kwargs,
    ) -> "TimeSeriesManager":
        """Deserialize the class. Must also call add_reference_counts after deserializing
        components.
        """
        time_series_dir = Path(parent_dir) / data["directory"]
        mgr = cls(**kwargs)
        if not mgr._read_only:
            mgr.serialize(src=time_series_dir, dst=mgr._ts_directory)
        else:
            mgr._ts_directory = time_series_dir
        return mgr

    def _handle_read_only(self):
        if self._read_only:
            raise ISOperationNotAllowed("Cannot modify time series in read-only mode.")


def clean_tmp_folder(folder: Path | str):
    shutil.rmtree(folder)
    logger.info("Wiped time series folder: {}", folder)
