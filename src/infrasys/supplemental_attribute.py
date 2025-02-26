"Defining base class for supplemental_attributes"

from typing import Any

from infrasys.base_quantity import BaseQuantity
from infrasys.models import InfraSysBaseModelWithIdentifers
from infrasys.serialization import (
    TYPE_METADATA,
    SerializedQuantityType,
    SerializedTypeMetadata,
    serialize_value,
)


class SupplementalAttribute(InfraSysBaseModelWithIdentifers):
    """Base class for supplemental attributes.
    Has a many-to-many relationship with components and can have time series attached.
    """

    def check_supplemental_attribute_addition(self) -> None:
        """Perform checks on the supplemental attribute before adding it to a system."""

    def model_dump_custom(self, *args, **kwargs) -> dict[str, Any]:
        """Custom serialization for this package"""

        refs = {}
        for x in self.model_fields:
            val = self._model_dump_field(x)
            if val is not None:
                refs[x] = val
        data = serialize_value(self, *args, **kwargs)
        data.update(refs)
        return data

    def _model_dump_field(self, field) -> Any:
        val = getattr(self, field)
        if isinstance(val, BaseQuantity):
            data = val.to_dict()
            data[TYPE_METADATA] = SerializedTypeMetadata(
                fields=SerializedQuantityType(
                    module=val.__module__,
                    type=val.__class__.__name__,
                ),
            ).model_dump()
            val = data
        else:
            val = None
        return val
