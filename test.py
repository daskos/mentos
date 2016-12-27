
from malefico.scheduler import Framework
from malefico.core.scheduler import MesosSchedulerDriver
import os
import getpass
from malefico.core.messages import  TaskID
sched = Framework()
driver = MesosSchedulerDriver(sched , "Queue", getpass.getuser())

driver.start()

from malefico.messages import PythonTask
from malefico.core.messages import Disk,Cpus,Mem ,TaskInfo,CommandInfo,Environment

import sys
executor = {
    "executor_id": {
        "value": "MinimalExecutor"
    },
    "name": "MinimalExecutor",
    "command": {
        "value": '%s %s' % (
            sys.executable, "~/workdir/mesos/malefico/malefico/executor.py"
            )

    }

}


#task  = TaskInfo(name='command-task', command=CommandInfo(value='echo $HOME',environment=env), resources=[Cpus(0.1), Mem(128), Disk(0)])
task = PythonTask( executor=executor,
                  fn=sum, args=[range(5)],
                  resources=[Cpus(0.1), Mem(128), Disk(0)])
sched.submit(task)
sched.wait()
import time
time.sleep(100000)

