"""Defines the Pydantic `pint.Quantity`."""

from __future__ import annotations

from numbers import Number
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler

import pint
from pint.facets.plain.quantity import PlainQuantity as Quantity
from pydantic_core import core_schema


class PydanticPintQuantity:
    """Pydantic-compatible annotation for validating and serializing `pint.Quantity` fields.

    This class allows Pydantic to handle fields that represent quantities with units,
    leveraging the `pint` library for unit conversion and validation.

    Parameters
    ----------
    units : str
        The base units of the Pydantic field. All input units must be convertible
        to these base units.
    ureg : pint.UnitRegistry, optional
        A custom Pint unit registry. If not provided, the default registry is used.
    ureg_contexts : str or list of str, optional
        A custom Pint context (or a list of contexts) for the default unit registry.
        All contexts are applied during validation and conversion.
    ser_mode : {"str", "dict"}, optional
        The mode for serializing the field. Can be one of:
        - `"str"`: Serialize to a string representation of the quantity (default in JSON mode).
        - `"dict"`: Serialize to a dictionary representation.
        By default, fields are serialized in Pydantic's `"python"` mode, which preserves
        the `pint.Quantity` type. In `"json"` mode, the field is serialized as a string.
    strict : bool, optional
        If `True` (default), forces users to specify units. If `False`, a value without
        units (provided by the user) is treated as having the base units of the field.

    Notes
    -----
    This class integrates with Pydantic's validation and serialization system to ensure
    that fields representing physical quantities are handled correctly with respect to units.
    """

    def __init__(
        self,
        units: str,
        *,
        ureg: pint.UnitRegistry | None = None,
        ser_mode: Literal["str", "dict"] | None = None,
        strict: bool = True,
    ):
        self.ser_mode = ser_mode.lower() if ser_mode else None
        self.strict = strict
        self.ureg = ureg if ureg else pint.UnitRegistry()
        self.units = self.ureg(units)

    def validate(
        self,
        input_value: Any,
        info: core_schema.ValidationInfo | None = None,
    ) -> Quantity:
        """Validate a `PydanticPintQuantity`.

        Parameters
        ----------
        input_value : Any
            The quantity to validate. This can be a dictionary containing keys `"magnitude"`
            and `"units"`, a string representing the quantity, or a `Number` or `Quantity`
            object that can be validated and converted to a `pint.Quantity`.
        info : core_schema.ValidationInfo, optional
            Additional validation information provided by the Pydantic schema. Default is `None`.

        Returns
        -------
        pint.Quantity
            The validated `pint.Quantity` with the correct units.

        Raises
        ------
        ValueError
            If validation fails due to one of the following reasons:
            - The provided `dict` does not contain the required `"magnitude"` and `"units"` keys.
            - No units are provided when strict mode is enabled.
            - The provided units cannot be converted to the base units.
            - An unknown unit is provided.
            - An invalid type is provided for the value.
        TypeError
            If the type is not supported.
        """
        # NOTE: `self.ureg` when passed returns the right type
        if not isinstance(input_value, Quantity):
            input_value = self.ureg(input_value)  # This convert string to numbers

        if isinstance(input_value, Number | list):
            input_value = input_value * self.units

        # At this point `input_value` should be a `pint.Quantity`.
        if not isinstance(input_value, Quantity):
            msg = f"{type(input_value)} not supported"
            raise TypeError(msg)
        try:
            input_value = input_value.to(self.units)
        except pint.DimensionalityError:
            msg = f"Dimension mismatch from {input_value.units} to {self.units}"
            raise ValueError(msg)
        return input_value

    def serialize(
        self,
        value: Quantity,
        info: core_schema.SerializationInfo | None = None,
    ) -> dict[str, Any] | str | Quantity:
        """
        Serialize a `PydanticPintQuantity`.

        Parameters
        ----------
        value : pint.Quantity
            The quantity to serialize. This should be a `pint.Quantity` object.
        info : core_schema.SerializationInfo, optional
            The serialization information provided by the Pydantic schema. Default is `None`.

        Returns
        -------
        dict, str, or pint.Quantity
            The serialized representation of the quantity.
            - If `ser_mode='dict'` or `info.mode='dict'` a dictionary with magnitude and units.

        Notes
        -----
        This method is useful when working with `PydanticPintQuantity` fields outside
        of Pydantic models, as it allows control over the serialization format
        (e.g., JSON-compatible representation).
        """
        mode = info.mode if info is not None else self.ser_mode
        if mode == "dict":
            return {
                "magnitude": value.magnitude,
                "units": f"{value.units}",
            }
        elif mode == "str" or mode == "json":
            return str(value)
        else:
            return value

    def __get_pydantic_core_schema__(
        self,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        _from_typedict_schema = {
            "magnitude": core_schema.typed_dict_field(
                core_schema.str_schema(coerce_numbers_to_str=True)
            ),
            "units": core_schema.typed_dict_field(core_schema.str_schema()),
        }

        validate_schema = core_schema.chain_schema(
            [
                core_schema.union_schema(
                    [
                        core_schema.is_instance_schema(Quantity),
                        core_schema.str_schema(coerce_numbers_to_str=True),
                        core_schema.typed_dict_schema(_from_typedict_schema),
                    ]
                ),
                core_schema.with_info_plain_validator_function(self.validate),
            ]
        )

        validate_json_schema = core_schema.chain_schema(
            [
                core_schema.union_schema(
                    [
                        core_schema.str_schema(coerce_numbers_to_str=True),
                        core_schema.typed_dict_schema(_from_typedict_schema),
                    ]
                ),
                core_schema.no_info_plain_validator_function(self.validate),
            ]
        )

        serialize_schema = core_schema.plain_serializer_function_ser_schema(
            self.serialize,
            info_arg=True,
        )

        return core_schema.json_or_python_schema(
            json_schema=validate_json_schema,
            python_schema=validate_schema,
            serialization=serialize_schema,
        )
