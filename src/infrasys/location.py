"""Defines models for geographic location."""

from typing_extensions import Annotated

from pydantic import Field

from infrasys import Component
from infrasys.supplemental_attribute_manager import SupplementalAttribute


class Location(Component):
    """Specifies geographic location."""

    name: Annotated[str, Field(frozen=True)] = ""
    x: float
    y: float
    crs: str | None = None


class GeographicInfo(SupplementalAttribute):
    """Specifies geographic location as a dictionary."""

    geojson: Annotated[
        dict[str, float], Field(description="Dictionary of geographical information")
    ]
