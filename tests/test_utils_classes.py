from infrasys import Component
from infrasys.utils.classes import get_all_concrete_subclasses


class Level1(Component):
    field1: int


class Level2(Level1):
    field2: int


class Level3(Level2):
    field3: int


def test_get_all_concrete_subclasses():
    assert get_all_concrete_subclasses(Level1) == {Level3}
    assert get_all_concrete_subclasses(Level2) == {Level3}
    assert not get_all_concrete_subclasses(Level3)
