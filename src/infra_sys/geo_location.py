"""Defines models for geographic location."""

from infra_sys.component_models import Component


class GeoLocation(Component):
    """Specifies geographic location."""

    x: float
    y: float
    crs: str | None = None
