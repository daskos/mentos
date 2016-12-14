import requests
from rx import Observable
from rx.concurrency import AsyncIOScheduler, NewThreadScheduler, ThreadPoolScheduler, TimeoutScheduler
import json
from rx.subjects import Subject
from attrdict import AttrDict
from malefico.corerx.operators import recordiorx
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import url_concat
from tornado.escape import json_decode as decode
from tornado import ioloop

from rx.concurrency import IOLoopScheduler
scheduler = IOLoopScheduler()

subscription_message = Subject()


def deserialize(x):
    return AttrDict(x)


def wrapper(gen):
    while True:
        try:
            yield next(gen)
        except StopIteration:
            raise
        except Exception as e:
            # or whatever kind of logging you want
            print("Connection to master dropped")
            pass


def resolve_url(master):

    if "hostname" in master["address"]:
        host = master["address"]["hostname"]
    elif "ip" in master["address"]:
        host = master["address"]["ip"]

    port = master["address"]["port"]

    return "http://{host}:{port}/api/v1/scheduler".format(host=host, port=port)


def get(subscription):
    url, message = subscription
    headers = {'content-type': 'application/json'}

    client = AsyncHTTPClient()
    god = Subject()

    def streaming_callback(chunk):
        god.on_next(chunk)

    def header_callback(response):
        pass
    r = HTTPRequest(url=url,
                    method='POST',
                    headers={'content-type': 'application/json'},
                    body=json.dumps(message),
                    streaming_callback=streaming_callback,
                    header_callback=header_callback,
                    request_timeout=1e100)
    try:
        client.fetch(r)
    except Exception as ex:
        raise Exception("Could not connect to server")
    return god.as_observable().retry()


import threading

url = 'http://localhost:5050/api/v1/scheduler'
d = {
    "type": "SUBSCRIBE",
    "subscribe": {
            "framework_info": {
                "user":  "username",
                "name":  "Example HTTP Framework"
            }
    }
}


def finish(x):
    print("finished %s on thread %s" % (x, threading.currentThread().name))


def run():
    root = Subject()

    subscription_master = root.map(
        decode).map(resolve_url).delay(1000)

    subscription_hot = Observable.combine_latest(subscription_master,
                                                 subscription_message,
                                                 lambda o1, o2: (o1, o2)).flat_map(get).retry().recordiorx().map(decode).map(deserialize)
    subsciption = subscription_hot.publish()

    heartbeat = subsciption.filter(lambda x: x["type"] == "HEARTBEAT")

    subscribed = subsciption.filter(lambda x: x["type"] == "SUBSCRIBED")

    offers = subsciption.filter(lambda x: x["type"] == "OFFERS")

    fuckme = subscribed.map(lambda x: x.subscribed.framework_id.value)
    heartbeat.subscribe(finish)
    subscribed.subscribe(finish)
    offers.subscribe(finish)

    fuckme.subscribe(subscription_message)
    subsciption.connect()
    subscription_message.on_next(d)

    from zkrx import MasterDetector
    detector = MasterDetector("localhost", root)
    detector.start()


def main():
    run()
    ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
