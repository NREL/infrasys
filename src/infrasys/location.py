"""Defines models for geographic location."""

from typing import Any

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

    geo_json: dict[str, Any] = Field(description="Dictionary of geographical information")

    @classmethod
    def example(cls) -> "GeographicInfo":
        return cls(
            geo_json={
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [125.6, 10.1]},
                "properties": {"name": "Dinagat Islands"},
            }
        )
