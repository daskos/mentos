from __future__ import print_function, division, absolute_import

from datetime import timedelta
from time import sleep
import uuid
import six
import socket
from tornado import gen
from tornado.gen import Return, TimeoutError
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from tornado.httputil import HTTPHeaders
from tornado.httputil import parse_response_start_line as parse_response
from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
import logging
from pyee import EventEmitter
from malefico.core.utils import log_errors, sync, get_master, master_info, get_http_master_url ,get_master_version
from collections import deque
from toolz import merge
from zoonado import Zoonado
from zoonado.exc import *
log = logging.getLogger(__name__)
from threading import Thread
from attrdict import AttrDict as Message
import socket

class SchedulerDriver(EventEmitter):
    def __init__(self, master, scheduler, name, user='' , failover_timeout=100, capabilities=[], implicit_acknowledgements=True, loop=None):

        self.loop = loop or IOLoop()
        self.status = "closed"
        self.master = master
        self.subscription_info = None
        self._leading_master = None
        self._leading_master_seq = None
        self._leading_master_master_info = None

        self.headers = {'content-type': 'application/json'}
        self.connection_successful = False

        self.detector = None
        self.buffer = deque()


        self.scheduler = scheduler
        self.framework={
            "user":user,
            "name":name,
            "capabilities":capabilities,
            "failover_timeout":failover_timeout,
            "hostname" : socket.gethostname()
        }

        self.implicit_acknowledgements = implicit_acknowledgements

        self._handlers = {
            "SUBSCRIBED": self.on_subscribed,
            "OFFERS": self.on_offers,
            "RESCIND": self.on_rescind,
            "UPDATE": self.on_update,
            "MESSAGE": self.on_message,
            "RESCIND_INVERSE_OFFER": self.on_rescind_inverse,
            "FAILURE": self.on_failure,
            "ERROR": self.on_error,
            "HEARTBEAT": self.on_heartbeat
        }

        # 
        super(SchedulerDriver, self).__init__()

    @property
    def framework_id(self):
        id = self.framework.get('id')
        return id and id.get('value')

    @framework_id.setter
    def framework_id(self, id):
        self.framework['id'] = dict(value=id)
 

    def start(self, **kwargs):
        """ Start scheduler running in separate thread """
        if hasattr(self, '_loop_thread'):
            return
        if not self.loop._running:

            self._loop_thread = Thread(target=self.loop.start)
            self._loop_thread.daemon = True
            self._loop_thread.start()
            while not self.loop._running:
                sleep(0.001)
        # print("heartbeat")
        pc = PeriodicCallback(lambda: None, 1000, io_loop=self.loop)
        self.loop.add_callback(pc.start)
        self.loop.add_callback(self._start)

    @gen.coroutine
    def _start(self, **kwargs):
        if not self._leading_master:
            self.loop.add_callback(self._detect_master)
        yield self.subscribe()

    @gen.coroutine
    def subscribe(self, retry_timeout=1):
        with log_errors():
            while not self._leading_master:
                yield gen.sleep(0.01)
            master_uri = (self._leading_master)
            client = AsyncHTTPClient()

            h = HTTPHeaders()

            def header_callback(response):
                try:
                    if "200 OK" in response:
                        self.connection_successful = True
                    else:
                        h.parse_line(response)
                    if self.connection_successful and "Mesos-Stream-Id" in h:
                        self.status = "connected"
                        self.mesos_stream_id = h["Mesos-Stream-Id"]
                except ValueError as ex:
                    log.warn("Problem parsing headers")

            try:

                message = dict(
                    type='SUBSCRIBE',
                    subscribe=dict(
                        framework_info=self.framework
                    ),
                )

                subscription_r = HTTPRequest(url=master_uri + "/api/v1/scheduler",
                                             method='POST',
                                             headers=self.headers,
                                             body=encode(message),
                                             streaming_callback=self._handlechunks,
                                             header_callback=header_callback,
                                             follow_redirects=False,
                                             request_timeout=1e100)

                yield client.fetch(subscription_r)
                self.status = 'connecting'
            except HTTPError as ex:
                if ex.code == 599:
                    log.warn("Disconnected from Master, will try to reconnect")
                    yield self.reconnect(self.framework, retry_timeout)
                if ex.code == 400:
                    log.warn("Got a 400 code from Master. Probably bad subscription request")
                    self.stop()
            except ConnectionRefusedError as ex:
                # TODO make this exponential
                log.warn("Problem connecting to the master. Will retry")
                yield self.reconnect( retry_timeout)
            except Exception as ex:
                log.error("Unhandled exception")
                log.exception(ex)
                # close

    @gen.coroutine
    def reconnect(self, retry_timeout=1):
        self._leading_master = None
        self.connection_successful = False
        self.buffer = deque()
        self.status = "disconnected"
        yield gen.sleep(retry_timeout)
        yield self.subscribe()

    @gen.coroutine
    def _handle_events(self, message):
        with log_errors():
            try:
                if message["type"]  in self._handlers:
                    self._handlers[message["type"]](message)
                else:
                    log.warn("Unhandled event %s" % message)
            except Exception as ex:
                log.warn("Problem dispatching event %s" % message)
                log.exception(ex)

    def on_error(self, event):
        message = event['message']
        self.scheduler.on_error(self, message)

    def on_heartbeat(self, event):
        message = "Heartbeat"
        self.emit("on_heartbeat",message)
        #self.scheduler.on_heartbeat(self, message)

    def on_subscribed(self, info):



        if self.framework_id:
            self.emit("on_reregistered", self._leading_master)
           # self.scheduler.reregistered(self, self._leading_master)
        else:
            self.framework_id = info["subscribed"]['framework_id']["value"]
            self.emit("on_registered",self.framework_id, self._leading_master)
            # self.scheduler.registered(
            #     self, self.framework_id,
            #     self._leading_master
            # )

    def on_offers(self, event):
        offers = event['offers']
        self.emit("on_registered", offers)
        # self.scheduler.resourceOffers(
        #     self, [offer for offer in offers]
        # )

    def on_rescind_inverse(self,event):
        pass
    def on_rescind(self, event):
        offer_id = event['offer_id']
        self.scheduler.offerRescinded(self, offer_id)

    def on_update(self, event):
        status = event['status']
        self.scheduler.statusUpdate(self, status)
        if self.implicit_acknowledgements:
            self.acknowledgeStatusUpdate(status)

    def on_message(self, message):
        executor_id = message['executor_id']
        agent_id = message['agent_id']
        data = message['data']
        self.scheduler.frameworkMessage(
            self, executor_id, agent_id, data
        )

    def on_failure(self, failure):
        agent_id = failure['agent_id']
        if 'executor_id' not in failure:
            self.scheduler.slaveLost(self, agent_id)
        else:
            self.scheduler.executorLost(
                self, failure['executor_id'],
                agent_id, failure['status']
            )



    @gen.coroutine
    def _handlechunks(self, chunk):
        """ Handle incoming  """
        with log_errors():
            try:
                self.buffer.append(chunk)
                length = self.buffer[0].split(b'\n', 1)[0]
                number = -len(length)
                length = int(length)
                i = 0
                while i < len(self.buffer) and number < length:
                    number += len(self.buffer[i])
                    i += 1

                if number < length:
                    return

                msgs = [self.buffer.popleft().split(b'\n', 1)[1]]
                number = len(msgs[0])

                while number < length:
                    msg = self.buffer.popleft()
                    number += len(msg)
                    msgs.append(msg)

                if number > length:
                    msg = msgs[-1]
                    length, message = msg[
                                      (length - number):], msg[:(length - number)]
                    msgs[-1] = message
                    self.buffer.appendleft(length)

                msg = decode(b''.join(msgs))
                yield self._handle_events(msg)
            except Exception as ex:
                log.warn("Problem parsing response from scheduler")

    def stop(self, timeout=10):
        """ stop
        """
        # XXX Not thread-safe?
        if self.status == 'closed':
            return

        if self.detector:
            log.warn("Terminiating detector")
            self.loop.add_callback(self.detector.close)

        log.warn("Terminating scheduler")
        self.loop.add_callback(self.loop.stop)

        self.status = "closed"

        while self.loop._running:
            sleep(0.001)

    @gen.coroutine
    def _detect_master(self, timeout=1):
        with log_errors():
            try:
                if "zk://" in self.master:
                    log.warn("Using Zookeeper for discovery")
                    quorum = ",".join([zoo[zoo.index('://') + 3:]
                                       for zoo in self.master.split(",")])
                    self.detector = Zoonado(quorum)
                    try:
                        yield self.detector.start()

                        @gen.coroutine
                        def children_changed(children):
                            yield gen.sleep(timeout)
                            current_state = yield self.detector.get_children("/mesos")
                            seq = get_master(current_state)
                            if seq == self._leading_master_seq and self.status == "disconnected":
                                log.warn("Master did not change, maybe just starting up, will watch for changes")
                                yield gen.sleep(timeout)
                            elif seq == -1:
                                log.warn("No master detected, will watch for changes")
                                self._leading_master = None
                                yield gen.sleep(timeout)
                            elif self.status in ("disconnected","closed"):
                                log.warn("New master detected")
                                self._leading_master_seq = seq
                                try:
                                    data = yield self.detector.get_data('/mesos/' + seq)
                                    self._leading_master_master_info = decode(data)
                                    self._leading_master = get_http_master_url(self._leading_master_master_info)
                                except NoNode as n:
                                    log.warn("Problem fetching Master node from zookeeper")

                        watcher = self.detector.recipes.ChildrenWatcher()
                        watcher.add_callback(
                            '/mesos', children_changed)
                        children = yield self.detector.get_children('/mesos')
                        yield children_changed(children)
                    except ConnectionLoss as n:
                        pass
                    except Exception as ex:
                        log.error("Unhandled exception in Detector")
                        yield self.detector.close()
                else:
                    # Two implementations are possible follow the 307 or do not, following has the advantage of getting more info
                    def get_actual_master(response):
                        if response.code == 307:
                            actual_master = six.moves.urllib.parse.urlparse(response.headers["location"])
                            self._leading_master_master_info = master_info(actual_master.netloc)
                            self._leading_master = get_http_master_url(self._leading_master_master_info)
                        else:
                            self._leading_master_master_info = master_info(self.master)
                            self._leading_master = get_http_master_url(self._leading_master_master_info)

                    potential_master = get_http_master_url(master_info(self.master))
                    check_master_r = HTTPRequest(url=potential_master + "/state",
                                                 method='GET',
                                                 headers=self.headers,
                                                 follow_redirects=False)

                    http_client = AsyncHTTPClient()
                    yield http_client.fetch(check_master_r, get_actual_master)

            except HTTPError as ex:
                if ex.code == "307":
                    pass
                else:
                    log.warn("Problem detecting master from url. Some problem with the network perhaps?")
            except Exception as ex:
                log.error("Problem resolving master:")
                log.exception(ex)

    def __enter__(self):
        if not self.loop._running:
            self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()

    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__,self.master,
            self._leading_master, self.framework)

    __repr__ = __str__
