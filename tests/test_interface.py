from mentos import interface

import pytest


@pytest.fixture()
def scheduler():
    return interface.Scheduler()


@pytest.fixture()
def executor():
    return interface.Executor()

# TODO should test calls


def test_calls_scheduler(scheduler):
    assert isinstance(scheduler, interface.Scheduler)

# TODO should test calls


def test_calls_executor(executor):
    assert isinstance(executor, interface.Executor)
