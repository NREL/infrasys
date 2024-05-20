"""This module defines basic unit quantities.

To create new Quantities for a given base unit, we just need to specify the
base unit as the second argument of `ureg.check`.
"""

from infrasys.base_quantity import BaseQuantity

# ruff:noqa
# fmt: off

class Distance(BaseQuantity): __base_unit__ = "meter"

class Voltage(BaseQuantity): __base_unit__ = "volt"

class Current(BaseQuantity): __base_unit__ = "ampere"

class Angle(BaseQuantity): __base_unit__ = "degree"

class ActivePower(BaseQuantity): __base_unit__ = "watt"

class Energy(BaseQuantity): __base_unit__ = "watthour"

class Time(BaseQuantity): __base_unit__ = "minute"

class Resistance(BaseQuantity): __base_unit__ = "ohm"
