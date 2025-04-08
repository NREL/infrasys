import re
from collections import OrderedDict
from datetime import timedelta

from dateutil.relativedelta import relativedelta

REGEX_DURATIONS = OrderedDict(
    {
        "milliseconds": r"^P0DT(\d+\.\d+)S$",
        "seconds": r"^P0DT(\d+)S$",
        "minutes": r"^P0DT(\d+)M$",
        "hours": r"^P0DT(\d+)H$",
        "days": r"^P(\d+)D$",
        "weeks": r"^P(\d+)W$",
        "months": r"^P(\d+)M$",
        "years": r"^P(\d+)Y$",
    }
)

DURATION_TO_TYPE = {
    "milliseconds": timedelta,
    "seconds": timedelta,
    "minutes": timedelta,
    "hours": timedelta,
    "days": timedelta,
    "weeks": timedelta,
    "months": relativedelta,
    "years": relativedelta,
}


def from_iso_8601(duration: str) -> timedelta | relativedelta:
    for name, regex in REGEX_DURATIONS.items():
        if match := re.match(regex, duration):
            if name == "milliseconds":
                value_float = float(match.group(1))
                if value_float % 1 != 0.0:
                    msg = "Milliseconds must bee divisible by 1000"
                    raise ValueError(msg)
                value = int(value_float) * 1000
            else:
                value = int(match.group(1))
            return DURATION_TO_TYPE[name](**{name: value})
    else:
        msg = f"No match found for {duration=}. "
        msg += "Check `REGEX_DURATIONS` to validate that the format is covered."
        raise ValueError(msg)


def to_iso_8601(duration: timedelta | relativedelta) -> str:
    """Convert a timedelta or relativedelta object to ISO 8601 duration format."""
    if not isinstance(duration, (timedelta, relativedelta)):
        msg = "Input must be a timedelta or relativedelta object."
        raise TypeError(msg)

    if isinstance(duration, relativedelta):
        years = duration.years or 0
        months = duration.months or 0
        days = duration.days or 0
        seconds = duration.hours * 3600 + duration.minutes * 60 + duration.seconds
        microseconds = duration.microseconds
    else:  # timedelta
        years = months = 0
        days = duration.days
        seconds = duration.seconds
        microseconds = duration.microseconds

    if years and not any([months, days, seconds, microseconds]):
        return f"P{years}Y"

    if months and not any([days, seconds, microseconds]):
        return f"P{months}M"

    if days and not any([seconds, microseconds]):
        if days % 7 == 0:
            return f"P{days // 7}W"
        return f"P{days}D"

    if not days and seconds % 3600 == 0 and not microseconds:
        hours = seconds // 3600
        return f"P0DT{hours}H"

    if not days and seconds % 60 == 0 and seconds % 3600 != 0 and not microseconds:
        minutes = seconds // 60
        return f"P0DT{minutes}M"

    # If not, we return seconds with fraction if milliseconds is provided.
    total_seconds = (
        duration.total_seconds()
        if isinstance(duration, timedelta)
        else seconds + microseconds / 1_000_000
    )
    if round(total_seconds, 3) == 0:
        msg = "Milliseconds must bee divisible by 1000"
        raise ValueError(msg)
    return f"P0DT{total_seconds:.3f}S"
