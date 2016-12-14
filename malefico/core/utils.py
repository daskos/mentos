from __future__ import print_function, division, absolute_import

import logging
from contextlib import contextmanager

from tornado import gen
from tornado.escape import json_decode as decode
from tornado.httpclient import HTTPClient

logger = logging.getLogger(__name__)



@contextmanager
def log_errors(pdb=False):
    try:
        yield
    except ( gen.Return):
        raise
    except Exception as e:
        logger.exception(e)
        if pdb:
            import pdb; pdb.set_trace()
        raise

def sync(loop, func, *args, **kwargs):
    """ Run coroutine in loop running in separate thread """
    if not loop._running:
        try:
            return loop.run_sync(lambda: func(*args, **kwargs))
        except RuntimeError:  # loop already running
            pass

def get_master_version(master_url):
    # I hate life or this
    try:
        client = HTTPClient()
        response = client.fetch("http://"+master_url+"/version")
        return decode(response.body)["version"]
    except Exception as ex:
        return None


def get_master(children):
    children = [child for child in children if child != 'log_replicas']
    if not children:
        return -1
    seq = min(children)
    return seq

def master_info(uri):
    master_info={"address":{}}
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
