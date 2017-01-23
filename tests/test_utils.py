
import pytest
import collections
from mentos import utils


@pytest.fixture
def d():
    return ["foo", 1, "bar", 9]


def test_drain_on_list():
    data = d()

    result = list(utils.drain(data))

    assert len(data)==0
    assert result== [9, "bar", 1, "foo"]


def test_drain_on_deque():
    data = collections.deque(d())

    result = list(utils.drain(data))

    assert len(data) == 0
    assert result == ["foo", 1, "bar", 9]


def test_drain_on_set():
    data = set(d())

    result = list(utils.drain(data))

    assert len(data) == 0
    assert set(result)== set(["foo", 1, "bar", 9])


def test_drain_on_dict():
    data = {"foo": 1, "bar": 9}

    result = {key: value for key, value in utils.drain(data)}

    assert len(data) == 0
    assert result== {"foo": 1, "bar": 9}