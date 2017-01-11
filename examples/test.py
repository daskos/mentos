from malefico.subscription import Subscription

from tornado import gen, ioloop
import socket
import getpass
sub = {
    "user": getpass.getuser(),
    "name": "test",
    "capabilities": [],
    "failover_timeout": 100000000,
    "hostname": socket.gethostname()
}


def handler(event):
    print(event)


@gen.coroutine
def b():
    a = Subscription(sub, "zk://localhost:2181", "/api/v1/scheduler",
                     timeout=1, loop=ioloop.IOLoop.current())
    yield a.start()

    try:
        a = yield a.send({})
    except Exception as ex:
        a = 1
    while 1:
        yield gen.sleep(1)

io_loop = ioloop.IOLoop.current()
# io_loop.set_blocking_log_threshold(0.1)
io_loop.run_sync(b)

import time
time.sleep(10000)
