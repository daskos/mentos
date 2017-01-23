import time

from mock import patch, Mock
from tornado import testing, concurrent
import pytest
from mentos import retry
from mentos import exceptions as exc


@pytest.mark.gen_test
def test_clear_timings():
    policy = retry.RetryPolicy.forever()

    request1 = Mock()
    request2 = Mock()

    yield policy.enforce(request1)
    yield policy.enforce(request1)
    yield policy.enforce(request2)

    assert len(policy.timings[id(request1)])== 2
    assert len(policy.timings[id(request2)])== 1

    policy.clear(request1)

    assert id(request1) not in policy.timings
    assert len(policy.timings[id(request2)])== 1

def test_once():
    policy = retry.RetryPolicy.once()

    assert policy.try_limit== 1
    assert policy.sleep_func([1, 2, 3])== None

def test_n_times():
    policy = retry.RetryPolicy.n_times(3)

    assert policy.try_limit== 3
    assert policy.sleep_func([1, 2, 3])== None

def test_forever():
    policy = retry.RetryPolicy.forever()

    assert policy.try_limit == None
    assert policy.sleep_func([1, 2, 3])== None

def test_exponential_backoff():
    policy = retry.RetryPolicy.exponential_backoff()

    assert policy.try_limit== None

    timings = []

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait== 1

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait== 2

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==4

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==8

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==16

def test_exponential_backoff_with_max_and_base():
    policy = retry.RetryPolicy.exponential_backoff(base=3, maximum=25)

    assert policy.try_limit== None

    timings = []

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==1

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==3

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==9

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==25

    wait = policy.sleep_func(timings)
    timings.append(wait)

    assert wait==25

@patch.object(retry, "time")
def test_until_elapsed( mock_time):
    state = {"now": time.time()}

    policy = retry.RetryPolicy.until_elapsed(timeout=4)

    def increment_time(*args):
        state["now"] += 1
        return state["now"]

    mock_time.time.side_effect = increment_time

    assert policy.try_limit== None

    timings = []

    wait = policy.sleep_func(timings)
    timings.append(state["now"])

    assert wait==3
    assert policy.sleep_func(timings)== 3
    assert policy.sleep_func(timings)== 2
    assert policy.sleep_func(timings)== 1
    assert policy.sleep_func(timings)== 0

@pytest.mark.gen_test
def test_failed_enforcement():
    policy = retry.RetryPolicy.once()

    request = Mock()

    yield policy.enforce(request)

    with pytest.raises(exc.FailedRetry):
        yield policy.enforce(request)

@pytest.mark.gen_test
def test_timeout_failure():

    def in_the_past(_):
        return -1

    request = Mock()

    policy = retry.RetryPolicy(try_limit=3, sleep_func=in_the_past)

    yield policy.enforce(request)

    with pytest.raises(exc.FailedRetry):
        yield policy.enforce(request)

@pytest.mark.gen_test()
def test_enforcing_wait_time_sleeps( mocker):
    f = concurrent.Future()
    f.set_result(None)
    mock_gen_sleep = mocker.patch.object(retry.gen, "sleep")

    mock_gen_sleep.return_value = f

    def in_the_future(_):
        return 60

    request = Mock()

    policy = retry.RetryPolicy(try_limit=3, sleep_func=in_the_future)

    yield policy.enforce(request)

    assert mock_gen_sleep.called==False

    yield policy.enforce(request)

    mock_gen_sleep.assert_called_once_with(60)