# Time Series

Infrastructure systems supports time series data expressed as a one-dimensional array of floats
using the class [SingleTimeSeries](#singe-time-series-api). Users must provide a `name`
that is typically the field of a component being modeled. For example, if the user has a time array
associated with the active power of a generator, they would assign
`name = "active_power"`.

Here is an example of how to create an instance of `SingleTimeSeries`:

```python
    import random
    time_series = SingleTimeSeries.from_array(
        data=[random.random() for x in range(24)],
        name="active_power",
        initial_time=datetime(year=2030, month=1, day=1),
        resolution=timedelta(hours=1),
    )
```

Users can attach their own attributes to each time array. For example,
there might be different profiles for different scenarios or model years.

```python
    time_series = SingleTimeSeries.from_array(
        data=[random.random() for x in range(24)],
        name="active_power",
        initial_time=datetime(year=2030, month=1, day=1),
        resolution=timedelta(hours=1),
        scenario="high",
        model_year="2035",
    )
```

## Resolution

Infrastructure systems support two types of objects for passing the resolution:
:class:`datetime.timedelta` and :class:`dateutil.relativedelta.relativedelta`.
These types allow users to define durations with varying levels of granularity
and semantic meaning.
While `timedelta` is best suited for precise, fixed-length
intervals (e.g., seconds, minutes, hours, days), `relativedelta` is more
appropriate for calendar-aware durations such as months or years, which do not
have a fixed number of days.

Internally, all durations, regardless of whether they are specified using
`timedelta` or `relativedelta`, are normalized and serialized into a strict [ISO
8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations).
This provides a consistent and standardized representation of
durations across the system, ensuring compatibility and simplifying transport,
storage, and validation.
For example, a `timedelta` of 1 month will be converted to the ISO format string
`P1M` and a `timedelta` of 1 hour will be converted to `P0DT1H`.

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

There are three possible things that will happen
with the storage class:

1. we create a file on-disk on a temporary location that will be erased (if the
   user did not serialize the system) once the process
   exit.
2. we create a file on a folder that is provided by the user. In this case, we
   check if the default file name exist in that folder. If so, we raise a warning
   else we create the file in the provided location.
3. we create an in-memory representation and it never gets written to disk and it gets wiped once the process exit.

The only time we preserve the storage is when we call `system.to_json` or `system.convert_storage`

Refer to the [Time Series API](#time-series-api) for more information.
