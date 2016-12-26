from __future__ import absolute_import, division, print_function

from binascii import a2b_base64, b2a_base64
from contextlib import contextmanager
import logging

from tornado import gen
from tornado.escape import json_decode as decode
from tornado.httpclient import HTTPClient

logger = logging.getLogger(__name__)


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
        logger.exception(e)
        if pdb:
            import pdb
            pdb.set_trace()
        raise

def get_master(children):
    children = [child for child in children if child != 'log_replicas']
    if not children:
        return -1
    seq = min(children)
    return seq


def master_info(uri):
    master_info = {"address": {}}
    hostport = uri.split(":")
    if len(hostport) == 2:
        master_info["address"]["hostname"] = hostport[0]
        master_info["address"]["port"] = int(hostport[1])
    else:
        master_info["address"]["hostname"] = hostport[0]
        master_info["address"]["port"] = 5050

    return master_info


def get_http_master_url(master):
    if "hostname" in master["address"]:
        host = master["address"]["hostname"]
    elif "ip" in master["address"]:
        host = master["address"]["ip"]

    port = master["address"]["port"]

    return "http://{host}:{port}".format(host=host, port=port)


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

    assert unit is not None, \
        'Unknown duration \'%s\'; supported units are %s' % (
            s, ','.join('\'%s\'' % n for n in POSTFIX)
        )

    n = float(s[:-len(postfix)])
    return n * unit

import signal
from contextlib import contextmanager


def partition(pred, iterable):
    trues, falses = [], []
    for item in iterable:
        if pred(item):
            trues.append(item)
        else:
            falses.append(item)
    return trues, falses


class TimeoutError(Exception):
    pass


@contextmanager
def timeout(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError("Timed out!")

    if seconds > 0:
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
    else:  # infinite timeout
        yield


class SignalHandler(object):

    def __init__(self, handler, signals=(signal.SIGINT, signal.SIGTERM)):
        self.handler = handler
        self.signals = signals
        self.original_handlers = {}

    def register(self):
        def signal_handler(signum, frame):
            self.release()
            self.handler()

        self.released = False
        for sig in self.signals:
            self.original_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, signal_handler)

    def release(self):
        if self.released:
            return False

        for sig in self.signals:
            signal.signal(sig, self.original_handlers[sig])

        self.released = True
        return True

    def __enter__(self):
        self.register()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        if exc_type:
            raise (exc_type, exc_value, traceback)


class Interruptable(object):

    def __init__(self):
        self.signal_handler = SignalHandler(self.stop)

    def start(self):
        self.signal_handler.register()
        return super(Interruptable, self).stop()

    def stop(self):
        self.signal_handler.release()
        return super(Interruptable, self).stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        self.join()
        if exc_type:
            raise (exc_type, exc_value, traceback)


class RemoteException(Exception):
    """ Remote Exception
    Contains the exception and traceback from a remotely run task
     - Include the original error message
     - Respond to try-except blocks with original error type
     - Include remote traceback
    """

    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback

    def __str__(self):
        return (str(self.exception) + "\n\n"
                "Traceback\n"
                "---------\n" +
                self.traceback)

    def __dir__(self):
        return sorted(set(dir(type(self)) +
                          list(self.__dict__) +
                          dir(self.exception)))

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            return getattr(self.exception, key)


exceptions = dict()


def remote_exception(exc, tb):
    """ Metaclass that wraps exception type in RemoteException """
    if type(exc) in exceptions:
        typ = exceptions[type(exc)]
        return typ(exc, tb)
    else:
        try:
            typ = type(exc.__class__.__name__,
                       (RemoteException, type(exc)),
                       {'exception_type': type(exc)})
            exceptions[type(exc)] = typ
            return typ(exc, tb)
        except TypeError:
            return exc