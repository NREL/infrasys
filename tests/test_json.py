import enum
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from uuid import uuid4

from infrasys.utils.json import ExtendedJSONEncoder


class Fruit(str, enum.Enum):
    APPLE = "apple"
    ORANGE = "orange"


def test_json_encoder():
    data = {
        "datetime": datetime.now(),
        "date": date.today(),
        "timedelta": timedelta(hours=1),
        "uuid": uuid4(),
        "path": Path(".").absolute(),
        "enum": Fruit.APPLE,
    }
    json.dumps(data, cls=ExtendedJSONEncoder)
