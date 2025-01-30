"Defining base class for supplemental_attributes"

from typing import Any
from infrasys.models import InfraSysBaseModelWithIdentifers
from infrasys.serialization import serialize_value


class SupplementalAttribute(InfraSysBaseModelWithIdentifers):
    """Base class for supplemental attributes.
    Has a many-to-many relationship with components and can have time series attached.
    """

    def model_dump_custom(self, *args, **kwargs) -> dict[str, Any]:
        """Custom serialization for this package"""
        return serialize_value(self, *args, **kwargs)
