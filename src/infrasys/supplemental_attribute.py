"Defining base class for supplemental_attributes"

from pydantic import Field
from typing_extensions import Annotated
from infrasys.models import InfraSysBaseModelWithIdentifers


class SupplementalAttribute(InfraSysBaseModelWithIdentifers):
    name: Annotated[str, Field(frozen=True)] = ""
