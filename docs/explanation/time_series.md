# Time Series
Infrastructure systems supports time series data expressed as a one-dimensional array of floats
using the class [SingleTimeSeries](#singe-time-series-api). Users must provide a `variable_name`
that is typically the field of a component being modeled. For example, if the user has a time array
associated with the active power of a generator, they would assign
`variable_name = "active_power"`.

Here is an example of how to create an instance of `SingleTimeSeries`:

```python
    import random
    time_series = SingleTimeSeries.from_array(
        data=[random.random() for x in range(24)],
        variable_name="active_power",
        initial_time=datetime(year=2030, month=1, day=1),
        resolution=timedelta(hours=1),
    )
```

Users can attach their own attributes to each time array. For example,
there might be different profiles for different scenarios or model years.

```python
    time_series = SingleTimeSeries.from_array(
        data=[random.random() for x in range(24)],
        variable_name="active_power",
        initial_time=datetime(year=2030, month=1, day=1),
        resolution=timedelta(hours=1),
        scenario="high",
        model_year="2035",
    )
```

## Behaviors
Users can customize time series behavior with these flags passed to the `System` constructor:

- `time_series_in_memory`: The `System` stores each array of data in an Arrow file by default. This
is a binary file that enables efficient storage and row access. Set this flag to store the data in
memory instead.
- `time_series_read_only`: The default behavior allows users to add and remove time series data.
Set this flag to disable mutation. That can be useful if you are de-serializing a system, won't be
changing it, and want to avoid copying the data.
- `time_series_directory`: The `System` stores time series data on the computer's tmp filesystem by
default. This filesystem may be of limited size. If your data will exceed that limit, such as what
is likely to happen on an HPC compute node, set this parameter to an alternate location (such as
`/tmp/scratch` on NREL's HPC systems).

Refer to the [Time Series API](#time-series-api) for more information.
