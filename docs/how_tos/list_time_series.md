# How to list existing time series data

Suppose that you have added multiple time series arrays to your components using differing
names and attributes. How can you see what is present?

This example assumes that a system with two generators and time series data has been serialized
to a file.

```python
from infrasys import Component, System

system = System.from_json("system.json")
for component in system.get_components(Component):
    for metadata in system.list_time_series_metadata(component):
        print(f"{component.label}: {metadata.label} {metadata.user_attributes}")

Generator.gen1: SingleTimeSeries.active_power {'scenario': 'high', 'model_year': '2030'}
Generator.gen1: SingleTimeSeries.active_power {'scenario': 'high', 'model_year': '2035'}
Generator.gen1: SingleTimeSeries.active_power {'scenario': 'low', 'model_year': '2030'}
Generator.gen1: SingleTimeSeries.active_power {'scenario': 'low', 'model_year': '2035'}
Generator.gen1: SingleTimeSeries.reactive_power {'scenario': 'high', 'model_year': '2030'}
Generator.gen1: SingleTimeSeries.reactive_power {'scenario': 'high', 'model_year': '2035'}
Generator.gen1: SingleTimeSeries.reactive_power {'scenario': 'low', 'model_year': '2030'}
Generator.gen1: SingleTimeSeries.reactive_power {'scenario': 'low', 'model_year': '2035'}
Generator.gen2: SingleTimeSeries.active_power {'scenario': 'high', 'model_year': '2030'}
Generator.gen2: SingleTimeSeries.active_power {'scenario': 'high', 'model_year': '2035'}
Generator.gen2: SingleTimeSeries.active_power {'scenario': 'low', 'model_year': '2030'}
Generator.gen2: SingleTimeSeries.active_power {'scenario': 'low', 'model_year': '2035'}
Generator.gen2: SingleTimeSeries.reactive_power {'scenario': 'high', 'model_year': '2030'}
Generator.gen2: SingleTimeSeries.reactive_power {'scenario': 'high', 'model_year': '2035'}
Generator.gen2: SingleTimeSeries.reactive_power {'scenario': 'low', 'model_year': '2030'}
Generator.gen2: SingleTimeSeries.reactive_power {'scenario': 'low', 'model_year': '2035'}
```

Now you can retrieve the exact instance you want.

```python
system.time_series.get(gen1, variable_name="active_power", scenario="high", model_year="2035").data
<pyarrow.lib.Int64Array object at 0x107a38d60>
[
  0,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  ...
  8774,
  8775,
  8776,
  8777,
  8778,
  8779,
  8780,
  8781,
  8782,
  8783
]
```
