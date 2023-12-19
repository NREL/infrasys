"""Defines models for geography coordinates."""

from infra_sys.component_models import Component


class GeographyCoordinates(Component):
    """Specifies geographic location."""

    latitude: float
    longitude: float
