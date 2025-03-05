"""Utility functions for classes."""

from typing import Type


_cached_classes: dict[Type, set[Type]] = {}


def get_all_concrete_subclasses(cls: Type) -> set[Type]:
    """Return all concrete subclasses of a class recursively.
    This excludes subclasses that have their own subclasses.
    Note that infrasys component types should not be used as both abstract and concrete, even
    though Python allows it.
    This keeps a cache and so dynamic changes to the class hierarchy will not be reflected.
    """
    cached_cls = _cached_classes.get(cls)
    if cached_cls is not None:
        return cached_cls

    subclasses = set()
    for subclass in cls.__subclasses__():
        if subclass.__subclasses__():
            subclasses.update(get_all_concrete_subclasses(subclass))
        else:
            subclasses.add(subclass)

    _cached_classes[cls] = subclasses
    return subclasses
