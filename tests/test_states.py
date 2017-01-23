import pytest

from mentos import states


@pytest.fixture
def fsm():
    return states.SessionStateMachine()


 
def test_defaults_to_closed_state(fsm):
    # fsm = fsm
    assert fsm.current_state==states.States.CLOSED


def test_fsm_equality(fsm):
    assert fsm == states.States.CLOSED
    assert fsm != states.States.SUBSCRIBED
    assert fsm != states.States.SUSPENDED

    fsm.transition_to(states.States.SUBSCRIBED)

    assert fsm != states.States.CLOSED
    assert fsm == states.States.SUBSCRIBED
    assert fsm != states.States.SUSPENDED


@pytest.mark.gen_test
def test_waiting_for_a_state(fsm):

    wait = fsm.wait_for(states.States.SUBSCRIBED)

    assert wait.done() is False

    fsm.transition_to(states.States.SUBSCRIBED)

    yield wait


@pytest.mark.gen_test
def test_waiting_for_any_of_a_few_states(fsm):
    wait = fsm.wait_for(
        states.States.SUBSCRIBED, states.States.SUBSCRIBING
    )

    assert wait.done() is False

    fsm.transition_to(states.States.SUBSCRIBED)

    yield wait

    fsm.transition_to(states.States.CLOSED)

    wait = fsm.wait_for(
        states.States.SUBSCRIBED, states.States.SUBSCRIBING
    )

    assert wait.done() is False

    fsm.transition_to(states.States.SUBSCRIBING)

    yield wait


@pytest.mark.gen_test
def test_waiting_on_current_state_yield_immediately(fsm):
    yield fsm.wait_for(states.States.CLOSED)


def test_valid_transitions(fsm):
    # closed/initial sessions can connect
    assert fsm == states.States.CLOSED
    fsm.transition_to(states.States.SUBSCRIBED)

    # connected sessions can be suspended
    assert fsm == states.States.SUBSCRIBED
    fsm.transition_to(states.States.SUSPENDED)

    # suspended session can reconnect
    assert fsm == states.States.SUSPENDED
    fsm.transition_to(states.States.SUBSCRIBED)

    # connected sessions can be closed (when closing)
    assert fsm == states.States.SUBSCRIBED
    fsm.transition_to(states.States.CLOSED)

    # closed sessions can reconnect as read-only
    assert fsm == states.States.CLOSED
    fsm.transition_to(states.States.SUBSCRIBING)

    # read-only sessions can become fully connected
    assert fsm == states.States.SUBSCRIBING
    fsm.transition_to(states.States.SUBSCRIBED)

    fsm.transition_to(states.States.SUSPENDED)

    # suspended sessions can reconnect as read-only
    assert fsm == states.States.SUSPENDED
    fsm.transition_to(states.States.SUBSCRIBING)

    # read-only sessions can be closed (when closing)
    assert fsm == states.States.SUBSCRIBING
    fsm.transition_to(states.States.CLOSED)

    fsm.transition_to(states.States.SUBSCRIBING)

    # read-only sessions can be suspended
    assert fsm == states.States.SUBSCRIBING
    fsm.transition_to(states.States.SUSPENDED)

    # suspended connections can be closed
    assert fsm == states.States.SUSPENDED
    fsm.transition_to(states.States.CLOSED)


def test_invalid_transitions(fsm):
    # TODO Need to check if this applies. closed sessions cannot be re-closed
    # with  pytest.raises(RuntimeError):
    #     fsm.transition_to(states.States.CLOSED)

    # closed sessions cannot be suspended
    with  pytest.raises(RuntimeError):
        fsm.transition_to(states.States.SUSPENDED)

    fsm.transition_to(states.States.SUBSCRIBED)

    # connected sessions cannot become read-only (suspended or closed first)
    with  pytest.raises(RuntimeError):
        fsm.transition_to(states.States.SUBSCRIBING)

    # connected sessions cannot be connected again
    with  pytest.raises(RuntimeError):
        fsm.transition_to(states.States.SUBSCRIBED)

    fsm.transition_to(states.States.SUSPENDED)

    # suspended sessions cannot be re-suspended
    with pytest.raises(RuntimeError):
        fsm.transition_to(states.States.SUSPENDED)

    fsm.transition_to(states.States.SUBSCRIBING)

    # read-only sessions cannot be made read-only twice
    with  pytest.raises(RuntimeError):
        fsm.transition_to(states.States.SUBSCRIBING)