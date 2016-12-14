import requests
from rx import Observable
from rx.concurrency import AsyncIOScheduler, NewThreadScheduler, ThreadPoolScheduler, TimeoutScheduler
import json
from rx.subjects import Subject
from attrdict import AttrDict
from malefico.core.operators import recordiorx
stream = Subject()


def decode(x):
    return json.loads(x.decode("ascii"))


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

    session = requests.Session()
    response = session.post(url, headers=headers,
                            data=json.dumps(message), stream=True, allow_redirects=False)

    if response.status_code == 307 and "location" in response.headers:
        url = response.headers["location"].replace("//", "http://")
        redirected = response = session.post(url, headers=headers,
                                             data=json.dumps(message), stream=True, allow_redirects=False)
        return Observable.from_iterable(wrapper(redirected.iter_content(1024))).retry()
    elif response.status_code == 200 and 'Mesos-Stream-Id' in response.headers:
        return Observable.from_iterable(wrapper(response.iter_content(1024))).retry()
    else:
        raise Exception("Could not get master")


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

a1 = Subject()
a2 = Observable.just(d)

#.map(deserialize)
a3 = a1.map(decode).map(resolve_url).delay(1000)

a = Observable.combine_latest(a3, a2, lambda o1, o2: (o1, o2)).flat_map(
    get).retry().recordiorx().map(decode).map(deserialize)
#a = Observable.just((url, d), scheduler=NewThreadScheduler())


b = a.publish()

heartbeat = b.filter(lambda x: x["type"] == "HEARTBEAT")

subscribed = b.filter(lambda x: x["type"] == "SUBSCRIBED")

offers = b.filter(lambda x: x["type"] == "OFFERS")

heartbeat.subscribe(finish)
subscribed.subscribe(finish)
offers.subscribe(finish)
b.connect()


from zkrx import MasterDetector
detector = MasterDetector("localhost", a1)
detector.start()

#a1.on_next(b'{"address": {"hostname": "localhost", "port": 5050}}')
import time
time.sleep(100000)
