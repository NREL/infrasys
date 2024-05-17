"""This module defines basic unit quantities.

To create new Quantities for a given base unit, we just need to specify the
base unit as the second argument of `ureg.check`.
"""

from infrasys.base_quantity import ureg, BaseQuantity

# ruff:noqa
# fmt: off

Distance = ureg.check(None, "meter")(BaseQuantity)
Voltage = ureg.check(None, "volt")(BaseQuantity)
Current = ureg.check(None, "ampere")(BaseQuantity)
Angle = ureg.check(None, "degree")(BaseQuantity)
ActivePower = ureg.check(None, "watt")(BaseQuantity)
Energy = ureg.check(None, "watthour")(BaseQuantity)
Time = ureg.check(None, "minute")(BaseQuantity)
Resistance = ureg.check(None, "ohm")(BaseQuantity)
