from binascii import a2b_base64
from binascii import b2a_base64
from os.path import abspath
from os.path import dirname
from os.path import join
import sys
import time
import uuid

from tornado.escape import json_encode as encode

from malefico.core.interface import Scheduler
from malefico.core.scheduler import MesosSchedulerDriver
from malefico.core.utils import encode_data

TASK_CPU = 0.2
TASK_MEM = 128
EXECUTOR_CPUS = 0.1
EXECUTOR_MEM = 32


def getResource(res, name):
    for r in res:
        if r["name"] == name:
            return r["scalar"]["value"]
    return 0.0


class WEEE(Scheduler):

    def __init__(self, executor):
        self.executor = executor
        self.one = False

    def on_heartbeat(self, driver, message):
        print(message)

    def on_offers(self, driver, offers):

        if self.one:
            driver.decline(offers)
            return
        self.one = True
        print(offers)
        filters = {'refuse_seconds': 5}

        for offer in offers:
            cpus = getResource(offer["resources"], 'cpus')
            mem = getResource(offer["resources"], 'mem')
            if cpus < TASK_CPU or mem < TASK_MEM:
                continue
            task_id = str(uuid.uuid4())
            task = {
                "task_id": {
                    "value": task_id
                },
                "agent_id": {
                    "value": offer["agent_id"]["value"]
                },
                "name": 'task {}'.format(task_id),
                "executor": executor,
                "data": encode_data('Hello from task {}!'.format(task_id).encode()),
                "resources": [
                    dict(name='cpus', type='SCALAR',
                         scalar={'value': TASK_CPU}),
                    dict(name='mem', type='SCALAR',
                         scalar={'value': TASK_MEM}),
                ]
            }

            driver.launch(offer["id"], [task], filters)


def on_rescinded(self, driver, offer_id):
    pass

executor = {
    "executor_id": {
        "value": "MinimalExecutor"
    },
    "name": "MinimalExecutor",
    "command": {
        "value": '%s %s' % (
            sys.executable,
            abspath(join(dirname(__file__)+"", 'executor.py'))
            )
    },
    "resources": [
        dict(name='mem', type='SCALAR', scalar={'value': EXECUTOR_MEM}),
        dict(name='cpus', type='SCALAR', scalar={'value': EXECUTOR_CPUS}),
    ]

}

sched = MesosSchedulerDriver('localhost', WEEE(executor), "Test", "arti")

sched.start()

time.sleep(100000)
sched.stop()
