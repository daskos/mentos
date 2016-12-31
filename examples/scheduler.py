import sys
import time
import uuid
from os.path import abspath, dirname, join

from malefico.interface import Scheduler
from malefico.scheduler import SchedulerDriver
from malefico.utils import encode_data

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
        pass

    def on_offers(self, driver, offers):

        if self.one:
            driver.decline([offer["id"] for offer in offers])
            return
        self.one = True
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

    def on_outbound_error(self, driver, response):
        pass

    def on_outbound_success(self, driver, response):
        pass

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
            abspath(join(dirname(__file__) + "", 'executor.py'))
        )
    },
    "resources": [
        dict(name='mem', type='SCALAR', scalar={'value': EXECUTOR_MEM}),
        dict(name='cpus', type='SCALAR', scalar={'value': EXECUTOR_CPUS}),
    ]

}

sched = SchedulerDriver(WEEE(executor), "Test", "arti")

sched.start(block=True)

# time.sleep(100000)
# sched.stop()
