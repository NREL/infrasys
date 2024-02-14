"""This module defines basic unit quantities."""

from infrasys.base_quantity import BaseQuantity

# ruff:noqa
# fmt: off

class Distance(BaseQuantity): __compatible_unit__ = "meter"

class Voltage(BaseQuantity): __compatible_unit__ = "volt"

class Current(BaseQuantity): __compatible_unit__ = "ampere"

class Angle(BaseQuantity): __compatible_unit__ = "degree"

class ActivePower(BaseQuantity): __compatible_unit__ = "watt"

class Energy(BaseQuantity): __compatible_unit__ = "watthour"

class Time(BaseQuantity): __compatible_unit__ = "minute"

class Resistance(BaseQuantity): __compatible_unit__ = "ohm"
