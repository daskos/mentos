from __future__ import absolute_import, division, print_function

from collections import deque
import logging
from threading import Thread
from time import sleep

from malefico.utils import log_errors, MasterInfo
from tornado import gen

from tornado.ioloop import IOLoop, PeriodicCallback
from malefico.states import SessionStateMachine, States
from malefico.retry import RetryPolicy
from malefico.connection import Connection
from tornado.httpclient import HTTPError
from malefico.exceptions import MasterRedirect, BadSubscription, ConnectionLost, ConnectError, BadMessage,NoLeadingMaster



log = logging.getLogger(__name__)

class Event(object):
    SUBSCRIBED = "SUBSCRIBED"
    HEARTBEAT = "HEARTBEAT"
    OFFERS = "OFFERS"
    RESCIND = "RESCIND"
    UPDATE= "UPDATE"
    MESSAGE = "MESSAGE"
    FAILURE = "FAILURE"
    ERROR= "ERROR"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    SHUTDOWN = "SHUTDOWN"
    KILL = "KILL"
    LAUNCH_GROUP = "LAUNCH_GROUP"
    LAUNCH = "LAUNCH"
    RESCIND_INVERSE_OFFER = "RESCIND_INVERSE_OFFER"


class Message(object):
    SUBSCRIBE = "SUBSCRIBE"
    UPDATE = "UPDATE"
    MESSAGE = "MESSAGE"
    ACCEPT = "ACCEPT"
    DECLINE= "DECLINE"
    REVIVE= "REVIVE"
    KILL= "KILL"
    SHUTDOWN= "SHUTDOWN"
    ACKNOWLEDGE= "ACKNOWLEDGE"
    RECONCILE= "RECONCILE"
    REQUEST = "REQUEST"

class Subscription(object):
    def __init__(self, framework, uri, api_path, event_handlers =None, timeout=75,
                 retry_policy=RetryPolicy.forever(), loop=None):

        self.loop = loop or IOLoop.current()

        self.connection = None
        self.state = SessionStateMachine()

        self.retry_policy = retry_policy

        self.connection_successful = False

        self.api_path = api_path

        self.event_handlers = event_handlers or {}

        self.buffer = deque()

        self.closing = False

        self.master_uri = uri
        self.master_info = MasterInfo(self.master_uri)

        self.mesos_stream_id = None
        self.framework = framework

        self.timeout = timeout


    @gen.coroutine
    def ensure_safe(self):
        safe_states = [States.SUBSCRIBED,States.SUBSCRIBING]
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
                if self.closing:
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
            except NoLeadingMaster as ex:
                self.connection = None
                endpoint = None

            if not endpoint:
                yield retry_policy.enforce()

            conn = yield self.make_connection(endpoint, self.api_path)

        old_conn = self.connection
        self.connection = conn
        log.warn("Connected to %s" % endpoint)
        if old_conn:
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

            yield self.connection.connect( request)
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
        conn = Connection(endpoint,self.mesos_stream_id, api_path, self._event_handler)
        try:
            yield conn.ping()
        except MasterRedirect as ex:
            log.warn("Master not leading")
            self.master_info.redirected_uri(ex.location)
            conn = None
        except ConnectionRefusedError as ex:
            conn = None
        except Exception:
            conn = None

        raise gen.Return(conn)

    @gen.coroutine
    def send(self, request):
        response = None
        while not response:
            yield self.retry_policy.enforce(request)
            yield self.ensure_safe()
            try:
                if "framework_id" not in request:
                    request["framework_id"] = self.framework["id"]
                response = yield self.connection.send(request)
                self.retry_policy.clear(request)
            except ConnectError as ex:
                self.state.transition_to(States.SUSPENDED)
                log.error(ex)
            except HTTPError as ex:
                exc = BadMessage("Bad call to Master, %s" % ex.response.body.decode())
                log.error(exc)
                raise exc

        raise gen.Return(response)

    @gen.coroutine
    def _event_handler(self,message):

        try:
            # Add special check to intercept framework_id
            if message.get("type", None) == Event.SUBSCRIBED:
                self.framework["id"] = message["subscribed"]["framework_id"]

            if message["type"] in self.event_handlers:
                _type = message['type']
                log.debug("Got event of type %s from %s" % (_type,self.master_info.info))
                if _type == Event.HEARTBEAT:
                    yield self.event_handlers[_type](message)
                elif _type == Event.SHUTDOWN:
                    yield self.event_handlers[_type]()
                else:
                    yield self.event_handlers[_type](message[_type.lower()])
            else:
                log.warn("Unhandled event %s" % message)
        except Exception as ex:
            log.warn("Problem dispatching event %s" % message)
            log.exception(ex)

    def close(self):
        self.closing = True

        self.state.transition_to(States.CLOSED)

        self.connection.close()
