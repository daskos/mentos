url = 'localhost:5050'
d = {
    "type": "SUBSCRIBE",
    "subscribe": {
        "framework_info": {
            "user": "username",
            "name": "Example HTTP Framework"
        }
    }
}

from malefico.core.interface import Scheduler
from malefico.core.scheduler import SchedulerDriver

TASK_CPU = 0.1
TASK_MEM = 32
EXECUTOR_CPUS = 0.1
EXECUTOR_MEM = 32


class WEEE(Scheduler):
    def __init__(self, executor):
        pass

    def on_heartbeat(self, driver, message):
        print(message)

    def on_offers(self, driver, offers):
        print(offers)

    def on_rescinded(self, driver, offer_id):
        pass


sched = SchedulerDriver(url, WEEE(1), "Test", "arti")

sched.start()
import time

time.sleep(100000)
sched.stop()
