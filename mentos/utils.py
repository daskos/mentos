from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
from binascii import a2b_base64, b2a_base64
from contextlib import contextmanager

from mentos.exceptions import NoLeadingMaster,NoRedirectException,DetectorClosed
from tornado import gen, ioloop
from tornado.escape import json_decode, json_encode
from zoonado import Zoonado

log = logging.getLogger(__name__)

decode = json_decode
encode = json_encode


def encode_data(data):
    return b2a_base64(data).strip().decode('ascii')


def decode_data(data):
    return a2b_base64(data)


from multiprocessing.pool import ThreadPool

_workers = ThreadPool(10)

def run_background(func, callback, args=(), kwds={}):
    def _callback(result):
        ioloop.IOLoop.instance().add_callback(lambda: callback(result))
    _workers.apply_async(func, args, kwds, _callback)


@contextmanager
def log_errors(pdb=False):# pragma: no cover
    try:
        yield
    except (gen.Return):
        raise
    except Exception as e:
        log.exception(e)
        if pdb:
            import pdb
            pdb.set_trace()
        raise


POSTFIX = {
    'ns': 1e-9,
    'us': 1e-6,
    'ms': 1e-3,
    'secs': 1,
    'mins': 60,
    'hrs': 60 * 60,
    'days': 24 * 60 * 60,
    'weeks': 7 * 24 * 60 * 60
}


def parse_duration(s):
    s = s.strip()
    unit = None
    postfix = None
    for postfix, unit in POSTFIX.items():
        if s.endswith(postfix):
            try:
                return float(s[:-len(postfix)]) * unit
            except ValueError:# pragma: no cover
                continue

    raise Exception('Unknown duration \'%s\'; supported units are %s' % (
        s, ','.join('\'%s\'' % n for n in POSTFIX)
    ))


class MasterInfo(object):
    detector = None

    def __init__(self, uri):

        self.uri = uri
        self.seq = None
        self.info = {"address": {}}
        self.closing = False

        if "zk://" in uri:
            log.warn("Using Zookeeper for Discovery")
            self.quorum = ",".join([zoo[zoo.index('://') + 3:]
                                    for zoo in self.uri.split(",")])
            self.detector = Zoonado(self.quorum,session_timeout=6000)

            ioloop.IOLoop.current().add_callback(self.detector.start)

        self.current_location = None

    def redirected_uri(self, uri):
        if not self.detector:
            self.uri = uri
        else:
            raise NoRedirectException("Using Zookeeper, cannot set a redirect url")



    @gen.coroutine
    def get_endpoint(self, path=""):

        if self.closing:
            raise DetectorClosed("Detecor is closed")

        if self.detector:

            children = yield self.detector.get_children("/mesos")
            children = [child for child in children if child != 'log_replicas']
            if not children: # pragma: no cover
                log.error("No leading Master found in zookeeper")
                raise NoLeadingMaster("No leading Master found in zookeeper")
            self.seq = min(children)
            data = yield self.detector.get_data('/mesos/' + self.seq)
            self.info = decode(data)
        else:
            host_port = self.uri.split(":")
            log.debug(host_port)
            if len(host_port) == 2:
                self.info["address"]["hostname"] = host_port[0]
                self.info["address"]["port"] = int(host_port[1])
            else:
                self.info["address"]["hostname"] = host_port[0]
                self.info["address"]["port"] = 5050

        log.debug(
            "Found new Master, info={info}".format(info=self.info))

        if "hostname" in self.info["address"]:
            host = self.info["address"]["hostname"]
        elif "ip" in self.info["address"]:# pragma: no cover
            host = self.info["address"]["ip"]

        port = self.info["address"]["port"]

        self.current_location  = "{host}:{port}".format(host=host, port=port)

        raise gen.Return(
            "http://{current_location}{path}".format(current_location=self.current_location, path=path))

    def close(self):
        if self.closing:
            return
        self.closing = True
        def on_complete(self):
           log.debug("Closed detector")
        run_background(self.detector.close, on_complete)



def drain(iterable):
    """
    Helper method that empties an iterable as it is iterated over.
    Works for:
    * ``dict``
    * ``collections.deque``
    * ``list``
    * ``set``
    """
    if getattr(iterable, "popleft", False):
        def next_item(coll):
            return coll.popleft()
    elif getattr(iterable, "popitem", False):
        def next_item(coll):
            return coll.popitem()
    else:
        def next_item(coll):
            return coll.pop()

    while True:
        try:
            yield next_item(iterable)
        except (IndexError, KeyError):
            raise StopIteration
