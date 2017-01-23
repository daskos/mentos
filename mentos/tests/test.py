import logging
import sys
import time
import uuid
from os.path import abspath, dirname, join

from mentos.interface import Scheduler
from mentos.scheduler import SchedulerDriver
from mentos.utils import encode_data

TASK_CPU = 0.2
TASK_MEM = 128
EXECUTOR_CPUS = 0.1
EXECUTOR_MEM = 32


def getResource(res, name):
    for r in res:
        if r["name"] == name:
            return r["scalar"]["value"]
    return 0.0


assert 1==1