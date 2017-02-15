from __future__ import unicode_literals


import logging

from collections import deque

from mentos.exceptions import (BadSubscription, ConnectError, ConnectionLost,
                               MasterRedirect,ConnectionRefusedError,OutBoundError)
from mentos.utils import decode, encode, log_errors
from six import raise_from
from six.moves.urllib.parse import urlparse
from tornado import concurrent, gen
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from tornado.httputil import HTTPHeaders

log = logging.getLogger(__name__)
payload_log = logging.getLogger(__name__ + ".payload")
if payload_log.level == logging.NOTSET:
    payload_log.setLevel(logging.INFO)


class Connection(object):
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Connection': 'close'
    }

    def __init__(self, endpoint, api_path, event_handler):

        self.endpoint = endpoint
        self.api_path = api_path
        self.subscription_client = AsyncHTTPClient()
        self.outbound_client = AsyncHTTPClient()

        self.event_handler = event_handler
        self.pending = {}

        self.buffer = deque()
        self.mesos_stream_id = None

        self.closing = False

        self.connection_successful = False
        self._headers = HTTPHeaders()

    def _parse_subscription_headers(self, response):
        try:
            if "200 OK" in response:
                self.connection_successful = True
            elif "400 Bad Request" in response:
                self.close()
            elif "HTTP/" not in response:
                self._headers.parse_line(response)
            if self.connection_successful and "Mesos-Stream-Id" in self._headers:
                self.mesos_stream_id = self._headers["Mesos-Stream-Id"].strip()
        except ValueError as ex:# pragma: no cover
            log.warn("Problem parsing headers")

    @gen.coroutine
    def connect(self, request):

        payload = encode(request)
        headers = dict(self.headers)
        headers['Content-Length'] = str(len(payload))

        http_request = HTTPRequest(url=self.endpoint + self.api_path,
                                   method='POST',
                                   headers=headers,
                                   body=payload,
                                   streaming_callback=self._handle_chunks,
                                   header_callback=self._parse_subscription_headers,
                                   follow_redirects=False,
                                   request_timeout=1e15)

        self.buffer = deque()
        self._headers = HTTPHeaders()

        try:
            yield self.subscription_client.fetch(http_request)
        except HTTPError as ex:
            if ex.code == 599:
                raise_from(ConnectionLost(
                    "Disconnected from endpoint, will try to reconnect"), None)
            if ex.code == 400:
                raise_from(BadSubscription(
                    "Got a 400 code from endpoint. Probably bad subscription request"), ex)
        except ConnectionRefusedError as ex:# pragma: no cover
            log.error("Problem subscribing: %s" % self.endpoint)
        except Exception as ex:# pragma: no cover
            log.error("Unhandled exception in subscription connection")
            log.exception(ex)

    def send(self, request):
        f = concurrent.Future()

        if self.closing:# pragma: no cover
            f.set_exception(ConnectError(self.endpoint))
            return f

        try:
            payload = encode(request)
            headers = dict(self.headers)
            headers['Content-Length'] = str(len(payload))
            http_request = HTTPRequest(
                url=self.endpoint + self.api_path,
                body=payload,
                method='POST',
                headers=headers,
            )
            if self.mesos_stream_id:
                headers['Mesos-Stream-Id'] = self.mesos_stream_id

            return self.outbound_client.fetch(http_request)
        except TypeError as ex:
            log.debug( "Could not serialize message {request} because {ex}".format(request=request,ex=ex))
            exc = OutBoundError(self.endpoint, request, ex)
            raise_from(exc, ex)
        except Exception as ex:  # pragma: no cover
            log.error("Unhandled exception in outbound connection")
            log.exception(ex)


    @gen.coroutine
    def ping(self, path=None):
        request = HTTPRequest(
            url=self.endpoint + (path or self.api_path),
            method='GET',
            headers=self.headers,
            follow_redirects=False,
            request_timeout = 100
        )
        try:
            yield self.outbound_client.fetch(request)
        except HTTPError as ex:# pragma: no cover
            if ex.code == 307:
                raise_from(MasterRedirect(
                    urlparse(ex.response.headers["location"]).netloc), None)
        except ConnectionRefusedError as ex:# pragma: no cover
            log.debug("Problem reaching: %s" % self.endpoint)
            raise ex
        except Exception as ex:# pragma: no cover
            log.debug("Unhandled exception when connecting to %s",
                      self.endpoint)
            raise ex

    def _handle_chunks(self, chunk):# pragma: no cover
        """ Handle incoming byte chunk stream """
        with log_errors():
            try:
                log.debug("Buffer length %s" % len(self.buffer))
                #TODO GOD #life
                if b"Failed to" in chunk and b'type' not in chunk:
                    log.warn("Got error from Master: %s" % chunk.decode())
                    return
                if b"No leader elected" in chunk:
                    log.warn(chunk.decode())
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

                self.event_handler(msg)

                # yield self.(msg)
            except Exception as ex:
                log.warn("Problem parsing response from endpoint. Might be a subscription error",ex)

    def close(self):
        if self.closing:
            return

        self.closing = True
        self.subscription_client.close()
        self.outbound_client.close()
