from __future__ import unicode_literals

import collections
import logging

from mentos.utils import drain
from tornado import concurrent

log = logging.getLogger(__name__)


class States(object):
    SUBSCRIBED = "subscribed"
    CLOSED = "closed"
    SUSPENDED = "suspended"
    SUBSCRIBING = "subscribing"


class SessionStateMachine(object):
    valid_transitions = set([
        (States.SUSPENDED, States.SUBSCRIBED),
        (States.SUSPENDED, States.CLOSED),
        (States.SUSPENDED, States.SUBSCRIBING),
        (States.SUBSCRIBED, States.SUSPENDED),
        (States.SUBSCRIBED, States.CLOSED),
        (States.CLOSED, States.CLOSED),
        (States.CLOSED, States.SUBSCRIBED),
        (States.CLOSED, States.SUBSCRIBING),
        (States.SUBSCRIBING, States.SUSPENDED),
        (States.SUBSCRIBING, States.CLOSED),
        (States.SUBSCRIBING, States.SUBSCRIBED),
    ])

    def __init__(self):
        self.current_state = States.CLOSED
        self.futures = collections.defaultdict(set)

    def transition_to(self, state):
        if (self.current_state, state) not in self.valid_transitions:
            raise RuntimeError(
                "Invalid session state transition: %s -> %s" % (
                    self.current_state, state
                )
            )

        log.debug("Session transition: %s -> %s", self.current_state, state)

        self.current_state = state

        for future in drain(self.futures[state]):
            if not future.done():
                future.set_result(None)

    def wait_for(self, *states):
        f = concurrent.Future()

        if self.current_state in states:
            f.set_result(None)
        else:
            for state in states:
                self.futures[state].add(f)

        return f

    def __eq__(self, state):
        return self.current_state == state

    def __ne__(self, state):
        return self.current_state != state
