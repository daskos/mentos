from __future__ import print_function, division, absolute_import

import logging
from collections import deque
from threading import Thread
from time import sleep

from tornado import gen
from tornado.escape import json_decode as decode
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.httputil import HTTPHeaders
from tornado.ioloop import IOLoop, PeriodicCallback
from malefico.core.utils import log_errors

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
log = logging.getLogger(__name__)


class MesosConnection(object):

    def __init__(self, leading_master=None, loop=None):

        self.loop = loop or IOLoop()
        self.status = "closed"
        self.connection_successful = False
        self.mesos_stream_id = None
        self.detector = None
        self.buffer = deque()
        self._handlers = {}
        self.leading_master = leading_master

    @gen.coroutine
    def _start(self, **kwargs):
        if not self.leading_master:
            self.loop.add_callback(self._detect_master)
        yield self.subscribe()

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
        pc = PeriodicCallback(lambda: None, 1000, io_loop=self.loop)
        self.loop.add_callback(pc.start)
        self.loop.add_callback(self._start)

    @gen.coroutine
    def subscribe(self, retry_timeout=1):
        with log_errors():

            while not self.leading_master:
                yield gen.sleep(0.01)

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
                        self.mesos_stream_id = h["Mesos-Stream-Id"].strip()
                except ValueError as ex:
                    log.warn("Problem parsing headers")

            try:
                subscription_r = self.gen_request(header_callback)
                yield client.fetch(subscription_r)
                self.status = 'connecting'
            except HTTPError as ex:
                if ex.code == 599:
                    log.warn("Disconnected from endpoint, will try to reconnect")
                    yield self.reconnect(retry_timeout)
                if ex.code == 400:
                    log.warn(
                        "Got a 400 code from endpoint. Probably bad subscription request")
                    self.stop()
            except ConnectionRefusedError as ex:
                # TODO make this exponential
                log.warn("Problem connecting to the endpoint. Will retry")
                yield self.reconnect(retry_timeout)
            except Exception as ex:
                log.error("Unhandled exception")
                log.exception(ex)
                # close

    def gen_request(self, handler):
        pass

    @gen.coroutine
    def reconnect(self, retry_timeout=1):
        self.leading_master = None
        self.connection_successful = False
        self.buffer = deque()
        self.status = "disconnected"
        yield gen.sleep(retry_timeout)
        yield self.subscribe()

    @gen.coroutine
    def _handlechunks(self, chunk):
        """ Handle incoming byte chunk stream """
        with log_errors():
            try:
                ## Ehm What ?
                if b"type" not in chunk:
                    log.warn(chunk)
                    return
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
                log.warn("Problem parsing response from endpoint")

    @gen.coroutine
    def _detect_master(self, timeout=1):
        pass

    def stop(self, timeout=10):
        """ stop
        """
        # TODO Not thread-safe?
        if self.status == 'closed':
            return
        if self.detector:
            log.warn("Terminating detector")
            self.loop.add_callback(self.detector.close)

        log.warn("Terminating scheduler")
        self.loop.add_callback(self.loop.stop)

        self.status = "closed"

        while self.loop._running:
            sleep(0.001)

    def __enter__(self):
        if not self.loop._running:
            self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
