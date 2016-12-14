from __future__ import print_function, division, absolute_import
from contextlib import contextmanager
from tornado.httpclient import HTTPClient
from tornado.escape import json_decode as decode
from tornado import gen
import logging
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

# def master_info(uri,info={}):
#     master_info={"address":{}}
#     hostport = uri.split(":")
#     if len(hostport) == 2:
#         master_info["address"]["hostname"] = hostport[0]
#         master_info["address"]["port"] = int(hostport[1])
#     else:
#         master_info["address"]["hostname"] = hostport[0]
#         master_info["address"]["port"] = 5050
#     if "version" in info:
#        master_info["version"] = info["version"]
#     else:
#         master_info["version"] = get_master_version(uri)
#     return master_info

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