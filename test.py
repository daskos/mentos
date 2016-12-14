url = 'zk://localhost:2181'
d = {
    "type": "SUBSCRIBE",
    "subscribe": {
        "framework_info": {
            "user": "username",
            "name": "Example HTTP Framework"
        }
    }
}

from malefico.core.scheduler import SchedulerDriver
from malefico.core.interface import Scheduler


class WEEE(object):
    def __init__(self):
        pass

    def on_registered(self, framework_id, master):
        pass

    def on_reregistered(self, framework_id, master):
        pass

    def on_disconnected(self, driver):
        pass

    def on_offers(self, offers):
        pass

    def on_rescinded(offer_id):
        pass

    def on_update(self, status):
        pass

    def on_message(self, executor_id, slave_id, message):
        pass

    def on_slave_lost(self, slave_id):
        pass

    def on_executor_lost(self, executor_id, slave_id, status):
        pass

    def on_error(self, message):
        pass


sched = SchedulerDriver(url, WEEE(), "Test", "arti")

sched.start()
import time

time.sleep(100000)
sched.stop()
