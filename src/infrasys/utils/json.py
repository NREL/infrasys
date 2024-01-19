"""JSON utilities"""

import enum
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from uuid import UUID


class ExtendedJSONEncoder(json.JSONEncoder):
    """Encodes additional types into JSON format."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return str(obj)

        if isinstance(obj, UUID):
            return str(obj)

        if isinstance(obj, timedelta):
            return obj.total_seconds()

        if isinstance(obj, enum.Enum):
            return obj.value

        if isinstance(obj, Path):
            return str(obj)

        return json.JSONEncoder.default(self, obj)
