"""Defines models for geographic location."""

from typing_extensions import Annotated

from pydantic import Field

from infrasys import Component


class Location(Component):
    """Specifies geographic location."""

    name: Annotated[str, Field(frozen=True)] = ""
    x: float
    y: float
    crs: str | None = None
