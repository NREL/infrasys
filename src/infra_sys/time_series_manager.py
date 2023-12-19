"""Manages time series arrays"""

import logging
from typing import Type
from uuid import UUID

from infra_sys.exceptions import ISDuplicateNames
from infra_sys.component_models import ComponentWithQuantities
from infra_sys.time_series_models import (
    SingleTimeSeries,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infra_sys.time_series_storage_base import TimeSeriesStorageBase
from infra_sys.in_memory_time_series_storage import InMemoryTimeSeriesStorage

logger = logging.getLogger(__name__)


class TimeSeriesManager:
    """Manages time series for a system."""

    def __init__(self, storage: TimeSeriesStorageBase | None = None):
        self._storage = storage or InMemoryTimeSeriesStorage()
        self._time_series_metadata: dict[UUID, TimeSeriesMetadata] = {}
        self._time_series_ref_counts: dict[UUID, int] = {}

        # TODO: enforce one resolution
        # TODO: create parsing mechanism? CSV, CSV + JSON
        # TODO: add delete methods that (1) don't raise if not found and (2) don't return anything?

    def add(self, time_series: TimeSeriesData, components: list[ComponentWithQuantities]) -> None:
        """Store a time series array for one or more components."""
        ts_type = type(time_series)
        metadata_type = ts_type.get_time_series_metadata_type()
        metadata = metadata_type.from_data(time_series)
        name = time_series.name

        for component in components:
            if component.has_time_series(name=name, time_series_type=ts_type):
                msg = f"{component.summary} already has a time series with {ts_type} {name}"
                raise ISDuplicateNames(msg)

        for component in components:
            self._storage.add_time_series(time_series)
            if time_series.uuid not in self._time_series_ref_counts:
                self._time_series_ref_counts[time_series.uuid] = 0
            self._time_series_ref_counts[time_series.uuid] += 1
            component.add_time_series_metadata(metadata)

    def get(
        self,
        component: ComponentWithQuantities,
        name: str,
        time_series_type: Type = SingleTimeSeries,
    ) -> TimeSeriesData:
        """Return a time series array."""
        metadata = component.get_time_series_metadata(name, time_series_type=time_series_type)
        return self._storage.get_time_series(metadata.uuid)

    def get_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        """Return a time series array."""
        return self._storage.get_time_series_by_uuid(uuid)

    def remove(
        self,
        components: list[ComponentWithQuantities],
        name: str,
        time_series_type=SingleTimeSeries,
    ) -> TimeSeriesData:
        """Remove all time series arrays matching the inputs.."""
        uuids = {}
        for component in components:
            metadata = component.get_time_series_metadata(name, time_series_type=time_series_type)
            if metadata.uuid in uuids:
                uuids[metadata.uuid] += 1
            else:
                uuids[metadata.uuid] = 1

        for uuid, count in uuids.items():
            if uuid not in self._time_series_ref_counts:
                msg = f"Bug: {uuid=} is not stored in self._time_series_ref_counts"
                raise Exception(msg)
            if count > self._time_series_ref_counts[uuid]:
                msg = (
                    f"Removing time series {name=} {time_series_type=} {uuid=}"
                    "will decrease the reference counts below 0."
                )
                raise Exception(msg)

        for component in components:
            component.remove_time_series_metadata(name, time_series_type=time_series_type)

        for uuid, count in uuids.items():
            self._time_series_ref_counts[uuid] -= count
            if self._time_series_ref_counts == 0:
                self._storage.remove_time_series(uuid)
                logger.info("Removed time series %s.%s", time_series_type, name)

    def remove_by_uuid(self, uuid: UUID) -> TimeSeriesData:
        """Remove a time series array and return it."""
        return self._storage.remove_time_series(uuid)

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

    def iter_metadata(self, time_series_type: None | Type = None):
        """Return an iterator over all time series metadata."""
        raise NotImplementedError("iter_time_series_metadata")

    def list_metadata(self, time_series_type: None | Type = None) -> list[TimeSeriesData]:
        """Return a list of all time series metadata."""
        return list(self.iter_metadata(time_series_type=time_series_type))
