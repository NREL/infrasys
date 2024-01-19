"""Defines models for geographic location."""

from infrasys.component_models import Component


class Location(Component):
    """Specifies geographic location."""

    x: float
    y: float
    crs: str | None = None
