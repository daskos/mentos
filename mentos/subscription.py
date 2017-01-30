from __future__ import absolute_import, division, print_function

import logging
from collections import deque

from mentos.connection import Connection
from mentos.exceptions import (BadMessage, BadSubscription, ConnectError,
                               ConnectionLost, MasterRedirect, NoLeadingMaster,ConnectionRefusedError)
from mentos.retry import RetryPolicy
from mentos.states import SessionStateMachine, States
from mentos.utils import MasterInfo, log_errors
from tornado import gen
from tornado.httpclient import HTTPError
from tornado.ioloop import IOLoop, PeriodicCallback

log = logging.getLogger(__name__)


class Event(object):
    SUBSCRIBED = "SUBSCRIBED"
    HEARTBEAT = "HEARTBEAT"
    OFFERS = "OFFERS"
    RESCIND = "RESCIND"
    UPDATE = "UPDATE"
    MESSAGE = "MESSAGE"
    FAILURE = "FAILURE"
    ERROR = "ERROR"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    SHUTDOWN = "SHUTDOWN"
    KILL = "KILL"
    LAUNCH_GROUP = "LAUNCH_GROUP"
    LAUNCH = "LAUNCH"
    RESCIND_INVERSE_OFFER = "RESCIND_INVERSE_OFFER"
    CLOSE = "CLOSE"


class Message(object):
    SUBSCRIBE = "SUBSCRIBE"
    UPDATE = "UPDATE"
    MESSAGE = "MESSAGE"
    ACCEPT = "ACCEPT"
    DECLINE = "DECLINE"
    REVIVE = "REVIVE"
    KILL = "KILL"
    SHUTDOWN = "SHUTDOWN"
    ACKNOWLEDGE = "ACKNOWLEDGE"
    RECONCILE = "RECONCILE"
    REQUEST = "REQUEST"


class Subscription(object):

    def __init__(self, framework, uri, api_path, event_handlers=None, timeout=75,
                 retry_policy=RetryPolicy.forever(), loop=None):

        self.loop = loop or IOLoop.current()

        self.connection = None
        self.state = SessionStateMachine()

        self.retry_policy = retry_policy

        self.api_path = api_path

        self.event_handlers = event_handlers or {}

        self.closing = False

        self.master_uri = uri
        self.master_info = MasterInfo(self.master_uri)

        self.mesos_stream_id = None

        # TODO I dont like doing this
        self.framework = framework
        self.tasks = None
        self.updates = None

        self.timeout = timeout

    @gen.coroutine
    def ensure_safe(self,safe_states = [States.SUBSCRIBED, States.SUBSCRIBING]):

        if self.state in safe_states:
            return

        yield self.state.wait_for(*safe_states)

    @gen.coroutine
    def ensure_subscribed(self):
        while not getattr(self.connection, 'mesos_stream_id', None):
            yield gen.sleep(0.1)
        self.mesos_stream_id = self.connection.mesos_stream_id
        self.state.transition_to(States.SUBSCRIBED)

    @gen.coroutine
    def start(self):
        pc = PeriodicCallback(lambda: None, 1000, io_loop=self.loop)
        self.loop.add_callback(pc.start)
        self.loop.add_callback(self.subscription_loop)
        yield self.ensure_safe()

    @gen.coroutine
    def subscription_loop(self):
        with log_errors():
            while not self.closing:
                yield self.state.wait_for(States.CLOSED, States.SUSPENDED)
                if self.closing:# pragma: no cover
                    break

                yield self.detect_master()

                self.loop.add_callback(self.ensure_subscribed)
                yield self.subscribe()

    @gen.coroutine
    def detect_master(self):
        conn = None

        retry_policy = RetryPolicy.exponential_backoff(maximum=self.timeout)

        while not conn:
            yield retry_policy.enforce()

            try:
                endpoint = yield self.master_info.get_endpoint()
            except NoLeadingMaster as ex:# pragma: no cover
                self.connection = None
                endpoint = None

            if not endpoint:# pragma: no cover
                yield retry_policy.enforce()

            conn = yield self.make_connection(endpoint, self.api_path)

        old_conn = self.connection
        self.connection = conn
        log.warn("Connected to %s" % endpoint)
        if old_conn:# pragma: no cover
            old_conn.close()

    @gen.coroutine
    def subscribe(self):
        try:
            self.state.transition_to(States.SUBSCRIBING)
            request = {
                'type': 'SUBSCRIBE',
                'subscribe': {
                    'framework_info': self.framework
                }
            }
            if "id" in self.framework:
                request["framework_id"] = self.framework["id"]

            if "executor_id" in self.framework:# pragma: no cover
                request["executor_id"] = self.framework["executor_id"]

            if self.tasks is not None:# pragma: no cover
                request["subscribe"]["unacknowledged_tasks"] = list(
                    self.tasks.values())

            if self.updates is not None:# pragma: no cover
                request["subscribe"]["unacknowledged_updates"] = list(
                    self.updates.values())

            yield self.connection.connect(request)
        except ConnectionLost as exc:
            log.warn("Lost connection to the Master, will try to resubscribe")
            self.connection.close()
            self.connection = None
            self.state.transition_to(States.SUSPENDED)

        except BadSubscription as exc:
            log.warn("Bad Subscription request, aborting")
            self.state.transition_to(States.CLOSED)
            self.close()

    @gen.coroutine
    def make_connection(self, endpoint, api_path):
        conn = Connection(endpoint,api_path, self._event_handler)
        try:
            yield conn.ping()
        except MasterRedirect as ex:# pragma: no cover
            if ex.location == self.master_info.current_location:
                log.warn("Leading Master not elected yet")
            else:# pragma: no cover
                log.warn("Master not leading")
                self.master_info.redirected_uri(ex.location)
            conn = None
        except ConnectionRefusedError as ex:# pragma: no cover
            conn = None
        except Exception:# pragma: no cover
            conn = None

        raise gen.Return(conn)

    @gen.coroutine
    def send(self, request):
        response = None
        #wait for safe state
        yield self.state.wait_for(States.SUBSCRIBED)
        while not response:
            yield self.retry_policy.enforce(request)
            yield self.ensure_safe()
            try:
                if "framework_id" not in request:
                    request["framework_id"] = self.framework["id"]
                response = yield self.connection.send(request)
                self.retry_policy.clear(request)
            except ConnectError as ex:# pragma: no cover
                self.state.transition_to(States.SUSPENDED)
                log.error(ex)
            except HTTPError as ex:
                exc = BadMessage("Bad call to Master, %s" %
                                 ex.response.body.decode())
                log.error(exc)
                raise exc
        raise gen.Return(response)

    def _event_handler(self, message):

        try:
            # Add special check to intercept framework_id
            if message.get("type", None) == Event.SUBSCRIBED:
                if "framework_id" in message["subscribed"]:
                    self.framework["id"] = message[
                        "subscribed"]["framework_id"]

            if message["type"] in self.event_handlers:
                _type = message['type']
                log.debug("Got event of type %s from %s" %
                          (_type, self.master_info.info))
                if _type == Event.HEARTBEAT:
                    self.event_handlers[_type](message)
                elif _type == Event.SHUTDOWN:# pragma: no cover
                    self.event_handlers[_type]()
                else:
                    self.event_handlers[_type](message[_type.lower()])
            else:# pragma: no cover
                log.warn("Unhandled event %s" % message)
        except Exception as ex:# pragma: no cover
            log.warn("Problem dispatching event %s" % message)
            log.exception(ex)

    def close(self):
        log.debug("Closing Subscription")
        self.closing = True

        if self.master_info.detector:
            log.debug("Closing Subscription Master Detector")
            self.loop.add_callback(self.master_info.detector.close)
        if self.connection:
            self.connection.close()
        self.state.transition_to(States.CLOSED)
