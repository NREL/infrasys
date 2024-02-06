# Time Series
Infrastructure systems supports time series data expressed as a one-dimensional array of floats.
Users must provide a `variable_name` that is typically the field of a component being modeled. For
example, if the user has a time array associated with the active power of a generator, they would assign
`variable_name = "active_power"`. Users can attach their own attributes to each time array. For example,
there might be different profiles for different model years.

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
