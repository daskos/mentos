
from malefico.scheduler import Framework
from malefico.core.scheduler import MesosSchedulerDriver
import os
import getpass
sched = Framework()
driver = MesosSchedulerDriver(os.getenv('MESOS_MASTER') or "localhost",sched , "Queue", getpass.getuser())

driver.start()

from malefico.messages import PythonTask
from malefico.core.messages import Disk,Cpus,Mem
task = PythonTask(fn=sum, args=[range(10)], name='satyr-task',
                  resources=[Cpus(0.1), Mem(128), Disk(512)])
sched.submit(task)
sched.wait()
pass
