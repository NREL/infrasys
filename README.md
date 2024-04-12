# infrasys

[![Main - CI](https://github.com/NREL/infrasys/workflows/Main%20-%20CI/badge.svg)](https://github.com/NREL/infrasys/actions/workflows/main_tests.yml)
[![codecov](https://codecov.io/gh/NREL/infrasys/branch/main/graph/badge.svg)](https://codecov.io/gh/NREL/infrasys)

This package implements a data store for components and time series in support of Python-based
modeling packages. While it is designed to support teams modeling transmission and distribution
systems for electrical grids, it can be used by any package that needs to store components
(e.g., generators and buses) that have quantities (e.g., power and voltage) which may vary over
time.

The package was inspired by
[InfrastructureSystems.jl](https://github.com/NREL-Sienna/InfrastructureSystems.jl)

## Benefits
- Stores components in data structures that provide fast lookup and iteration by type and name.
- Provides extendable data models that enable validation and unit conversion through
[pint](https://pint.readthedocs.io/en/stable/).
- Manages time series data efficiently. Data is only loaded into system memory when needed by
the user application.
- Manages serialization and de-serialization of components to JSON, including automatic handling of
nested objects.
- Enables data model migration.

## Package Developer Guide
🚧

## Installation
```
$ pip install git+ssh://git@github.com/NREL/infrastructure_systems.git@main
```

## Developer installation
```
$ pip install -e ".[dev]"
```

Please install `pre-commit` so that your code is checked before making commits.
```
$ pre-commit install
```
