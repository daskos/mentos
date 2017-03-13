from __future__ import print_function

import logging
import sys
import time
from threading import Thread

from mentos.executor import ExecutorDriver
from mentos.interface import Executor
from mentos.utils import decode_data


class MinimalExecutor(Executor):

    def on_launch(self, driver, task):

        def run_task(task):
            update = {
                'task_id': {
                    'value': task['task_id']['value']
                },
                'state': 'TASK_RUNNING',
                'timestamp': int(time.time())
            }
            driver.update(update)

            print(decode_data(task['data']), file=sys.stderr)
            time.sleep(5)

            update = {
                'task_id': {
                    'value': task['task_id']['value']
                },
                'state': 'TASK_FINISHED',
                'timestamp': time.time()
            }
            driver.update(update)

        thread = Thread(target=run_task, args=(task,))
        thread.daemon = True
        thread.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    driver = ExecutorDriver(MinimalExecutor())
    driver.start(block=True)
