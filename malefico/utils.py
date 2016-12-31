from __future__ import absolute_import, division, print_function

from binascii import a2b_base64, b2a_base64
from contextlib import contextmanager
import logging

from tornado import gen

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
