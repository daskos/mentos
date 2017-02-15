from __future__ import absolute_import, division, print_function

import logging
from collections import deque

from mentos.connection import Connection
from mentos.exceptions import (BadMessage, BadSubscription, ConnectError,DetectorClosed,
                               ConnectionLost, MasterRedirect, NoLeadingMaster,ConnectionRefusedError,OutBoundError,FailedRetry,BadRequest)
from mentos.retry import RetryPolicy
from mentos.states import SessionStateMachine, States
from mentos.utils import MasterInfo, log_errors
from tornado import gen
from tornado.httpclient import HTTPError
from tornado.ioloop import IOLoop, PeriodicCallback
from six import raise_from
from time import sleep
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
    OUTBOUND_SUCCESS = "OUTBOUND_SUCCESS"
    OUTBOUND_ERROR = "OUTBOUND_ERROR"

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
        if self.state.current_state == States.SUBSCRIBING:
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

        self.retry_policy = RetryPolicy.exponential_backoff(maximum=self.timeout)

        while not conn:
            yield self.retry_policy.enforce()
            import tornado
            try:
                endpoint = yield self.master_info.get_endpoint()
            except NoLeadingMaster as ex:# pragma: no cover
                self.connection = None
                endpoint = None
            except DetectorClosed as ex:
                self.connection = None
                endpoint = None

            if not endpoint:# pragma: no cover
                yield self.retry_policy.enforce()

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
    def send(self, request,retry_policy=RetryPolicy.n_times(3)):
        response = None
        #wait for safe state
        yield self.state.wait_for(States.SUBSCRIBED)
        errors = []
        while not response:
            try:

                yield retry_policy.enforce(request)
                yield self.ensure_safe()

                if "framework_id" not in request:
                    request["framework_id"] = self.framework["id"]
                response = yield self.connection.send(request)
            except HTTPError as ex:
                if ex.code == 400:
                    ex = BadRequest(ex.response.body)
                    log.debug("Bad request {request}, {ex}".format(request=request,ex=ex))
                    errors.append(ex)
            except OutBoundError as ex:# pragma: no cover
                #TODO question marc
                #self.state.transition_to(States.SUSPENDED)
                log.debug("Bad outbound message {request} because {ex}".format(request=request, ex=ex))
                errors.append(ex)
            except FailedRetry as ex:
                log.error("Ran out of retries for {request}, last error {ex}".format(request=request,ex=errors[-1]))
                self.outbound_error(OutBoundError(self.connection.endpoint, request, errors))
                break
            else:
                retry_policy.clear(request)
                self.outbound_succes(request)

        raise gen.Return(response)

    def outbound_error(self,ex):
        self._event_handler({"type": Event.OUTBOUND_ERROR,
                             "outbound_error": {"request": ex.request, "endpoint": ex.endpoint, "error": ex.errors}})
    def outbound_succes(self,request):
        self._event_handler({"type": Event.OUTBOUND_SUCCESS, "outbound_success": {"request": request}})

    def _event_handler(self, message):

        try:
            # Add special check to intercept framework_id
            if message.get("type", None) == Event.SUBSCRIBED:
                if "framework_id" in message["subscribed"]:
                    self.framework["id"] = message[
                        "subscribed"]["framework_id"]
                # Add special check to ensure subscribed for executor
                if self.state.current_state==States.SUBSCRIBING and "executor_info" in message["subscribed"]:# pragma: no cover
                    self.state.transition_to(States.SUBSCRIBED)

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
            self.master_info.close()
            #self.loop.add_callback(self.master_info.detector.close,0.1)
            # while self.master_info.detector.session.state.current_state!="CLOSED":  # pragma: no cover
            #     sleep(0.1)
        if self.connection:
            self.connection.close()
        self.state.transition_to(States.CLOSED)
