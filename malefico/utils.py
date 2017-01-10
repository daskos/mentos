from __future__ import absolute_import, division, print_function, unicode_literals

from binascii import a2b_base64, b2a_base64
from contextlib import contextmanager
from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
from tornado import gen,ioloop

from toolz import compose
from malefico.exceptions import NoLeadingMaster
import logging



log = logging.getLogger(__name__)


def encode_data(data):
    return b2a_base64(data).strip().decode('ascii')


def decode_data(data):
    return a2b_base64(data)


@contextmanager
def log_errors(pdb=False):
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
    for n, u in POSTFIX.items():
        if s.endswith(n):
            unit = u
            postfix = n
            break

    #TODO exceptions?
    assert unit is not None, \
        'Unknown duration \'%s\'; supported units are %s' % (
            s, ','.join('\'%s\'' % n for n in POSTFIX)
        )

    n = float(s[:-len(postfix)])
    return n * unit


from zoonado import Zoonado


class MasterInfo(object):
    detector = None

    def __init__(self, uri):

        self.uri = uri
        self.seq = None
        self.info = {"address": {}}

        if "zk://" in uri:
            log.warn("Using Zookeeper for Discovery")
            self.quorum = ",".join([zoo[zoo.index('://') + 3:]
                                    for zoo in self.uri.split(",")])
            self.detector = Zoonado(self.quorum)

            ioloop.IOLoop.current().add_callback(self.detector.start)


    def redirected_uri(self,uri):
        self.uri = uri

    @gen.coroutine
    def get_endpoint(self,path=""):
            if self.detector:

                children = yield self.detector.get_children("/mesos")
                children = [child for child in children if child != 'log_replicas']
                if not children:
                    log.error("No leading Master found in zookeeper")
                    raise NoLeadingMaster("No leading Master found in zookeeper")
                self.seq = min(children)
                data = yield self.detector.get_data('/mesos/' + self.seq)
                self.info = decode(data)
            else:
                host_port = self.uri.split(":")
                if len(host_port) == 2:
                    self.info["address"]["hostname"] = host_port[0]
                    self.info["address"]["port"] = int(host_port[1])
                else:
                    self.info["address"]["hostname"] = host_port[0]
                    self.info["address"]["port"] = 5050

            log.debug("Found new leading Master, info={info}".format(info=self.info))

            if "hostname" in self.info["address"]:
                host = self.info["address"]["hostname"]
            elif "ip" in self.info["address"]:
                host = self.info["address"]["ip"]

            port = self.info["address"]["port"]

            raise gen.Return("http://{host}:{port}{path}".format(host=host, port=port,path=path))



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
