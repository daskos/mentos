from __future__ import print_function

import sys
import time
from threading import Thread

from malefico.core.interface import Executor
from malefico.core.executor import MesosExecutorDriver
from tornado.escape import json_encode as encode
from malefico.core.utils import  decode_data
import uuid
import sys



class MinimalExecutor(Executor):
    def on_launch(self, driver, task):
        def run_task(task):
            update = {
                "task_id":{
                    "value":task["task_id"]["value"]
                },
                "state": 'TASK_RUNNING',
                "timestamp" :  int(time.time())
            }
            #driver.message(b"godd")
            driver.update(update)


            print(decode_data(task["data"]), file=sys.stderr)
            time.sleep(100)

            update = {
                "task_id": {
                    "value": task["task_id"]["value"]
                },
                "state": 'TASK_FINISHED',
                "timestamp": time.time()
            }

            driver.update(update)

        thread = Thread(target=run_task, args=(task,))
        thread.daemon = True
        thread.start()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    time.sleep(10)
    driver = MesosExecutorDriver(MinimalExecutor())
    driver.start()
    time.sleep(100000)