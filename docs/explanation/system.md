# System
The System class provides a data store for components and time series data.

Refer to the [System API](#system-api) for complete information.

## Items to consider for parent packages

### Composition vs Inheritance
Parent packages must choose one of the following:

1. Derive a custom System class that inherits from `infrasys.System`. Re-implement methods
as desired.

    - Reimplement `System.add_components` in order to perform custom validation
      or custom behavior. For example, a package may implement a load that
      contains a bus. Here are some possible desired behaviors when adding the
      load to the system if its bus is not already attached to the system:

      - Raise an exception.
      - Automatically add the bus to the system.

    - Reimplement `System.serialize_system_attributes` and `System.deserialize_system_attributes`.
      `infrasys` will call those methods during `to_json` and `from_json` and serialize/de-serialize
      the contents.

    - Reimplement `System.data_format_version` and `System.handle_data_format_upgrade`. `infrasys`
      will call the upgrade function if it detects a version change during de-serialization.

2. Implement an independent System class and compose the `infrasys.System`. This can be beneficial
if you want to make the underlying system opaque to users.

3. Use `infrasys.System` directly. This is probably not what most users want.
