from __future__ import print_function

import sys
import time
from threading import Thread

from malefico.executor import ExecutorDriver
from malefico.interface import Executor
from malefico.utils import decode_data


class MinimalExecutor(Executor):

    def on_launch(self, driver, task):
        def run_task(task):
            update = {
                "task_id": {
                    "value": task["task_id"]["value"]
                },
                "state": 'TASK_RUNNING',
                "timestamp":  int(time.time())
            }
            driver.update(update)

            print(decode_data(task["data"]), file=sys.stderr)
            time.sleep(5)

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
    # debug
    # time.sleep(20)
    driver = ExecutorDriver(MinimalExecutor())
    driver.start(block=True)
    # time.sleep(100000)
