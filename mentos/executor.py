from __future__ import absolute_import, division, print_function

import logging
import os
import signal
import time
import uuid
from os import environ as env
from threading import Thread
from time import sleep

from mentos.exceptions import ExecutorException
from mentos.subscription import Event, Subscription
from mentos.utils import decode_data, encode_data, parse_duration
from toolz import merge
from tornado.ioloop import IOLoop

log = logging.getLogger(__name__)


class ExecutorDriver():

    def __init__(self, executor, handlers=None, loop=None):
        """

        Args:
            executor ():
            loop ():
        """
        self.loop = loop or IOLoop()

        self.master = env.get('MESOS_AGENT_ENDPOINT')

        self.framework_id = dict(value=env.get('MESOS_FRAMEWORK_ID'))
        self.executor_id = dict(value=env.get('MESOS_EXECUTOR_ID'))

        self.framework = {
            "id": self.framework_id,
            "framework_id": self.framework_id,
            "executor_id": self.executor_id
        }

        grace_shutdown_period = env.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD')
        if grace_shutdown_period:# pragma: no cover
            self.grace_shutdown_period = parse_duration(grace_shutdown_period)
        else:
            self.grace_shutdown_period = 0.0

        self.checkpoint = bool(env.get('MESOS_CHECKPOINT'))
        self.local = bool(env.get('MESOS_LOCAL',True))

        self.executor = executor
        self.framework_info = None
        self.executor_info = None

        self.executor = executor
        self.handlers = merge({
            Event.SUBSCRIBED: self.on_subscribed,
            Event.CLOSE: self.on_close,
            Event.MESSAGE: self.on_message,
            Event.ERROR: self.on_error,
            Event.ACKNOWLEDGED: self.on_acknowledged,
            Event.KILL: self.on_kill,
            Event.LAUNCH_GROUP: self.on_launch_group,
            Event.LAUNCH: self.on_launch,
            Event.SHUTDOWN: self.on_shutdown,
            Event.OUTBOUND_SUCCESS: self.on_outbound_success,
            Event.OUTBOUND_ERROR: self.on_outbound_error
        }, handlers or {})

        self.subscription = Subscription(self.framework, self.master, "/api/v1/executor",
                                         self.handlers, loop=self.loop)

        self.subscription.tasks = {}
        self.subscription.updates = {}

    def start(self, block=False, **kwargs):
        """ Start executor running in separate thread """
        # if hasattr(self, '_loop_thread'):
        #     return
        if not self.loop._running:

            self._loop_thread = Thread(target=self.loop.start)
            self._loop_thread.daemon = True
            self._loop_thread.start()
            while not self.loop._running:# pragma: no cover
                sleep(0.001)

        self.loop.add_callback(self.subscription.start)
        if block:# pragma: no cover
            self._loop_thread.join()

    def stop(self):
        """ stop
        """
        log.debug("Terminating Scheduler Driver")
        self.subscription.close()
        self.loop.add_callback(self.loop.stop)
        while self.loop._running:# pragma: no cover
            sleep(0.1)

    def update(self, status):
        """
        """
        if 'timestamp' not in status:
            status['timestamp'] = int(time.time())

        if 'uuid' not in status:
            status['uuid'] = encode_data(uuid.uuid4().bytes)

        if 'source' not in status:
            status['source'] = 'SOURCE_EXECUTOR'

        payload = {
            "type": "UPDATE",
            "framework_id": self.framework_id,
            "executor_id": self.executor_id,
            "update": {
                "status": status
            }
        }
        self.loop.add_callback(self.subscription.send, payload)
        logging.debug('Executor sends status update {} for task {}'.format(
            status["state"], status["task_id"]))

    def message(self, message):
        """
        """
        payload = {
            "type": "MESSAGE",
            "framework_id": self.framework_id,
            "executor_id": self.executor_id,
            "message": {
                "data": encode_data(message)
            }
        }
        self.loop.add_callback(self.subscription.send, payload)
        logging.debug('Driver sends framework message {}'.format(message))

    def on_subscribed(self, info):
        executor_info = info['executor_info']
        framework_info = info['framework_info']
        agent_info = info['agent_info']
        if executor_info['executor_id'] != self.executor_id:# pragma: no cover
            raise ExecutorException("Mismatched executor_id's")

        if framework_info['id'] != self.framework_id:# pragma: no cover
            raise ExecutorException("Mismatched framework_ids")

        if self.executor_info is None or self.framework_info is None:
            self.executor_info = executor_info
            self.framework_info = framework_info
            self.executor.on_registered(
                self, executor_info,
                self.framework_info, agent_info
            )
        else:# pragma: no cover
            self.executor.on_reregistered(self, agent_info)

        log.debug("Subscribed with info {}".format(info))

    def on_close(self):
        if not self.checkpoint:
            if not self.local:# pragma: no cover
                self._delay_kill()
            self.executor.on_shutdown(self)

        log.debug("Got close command")

    def on_launch_group(self, event):
        task_info = event['task']
        task_id = task_info['task_id']['value']
        if task_id in self.subscription.tasks:# pragma: no cover
            raise ExecutorException("Task Exists")
        self.subscription.tasks[task_id] = task_info
        self.executor.on_launch(self, task_info)
        log.debug("Got launch group command {}".format(event))

    def on_launch(self, event):

        task_info = event['task']
        task_id = task_info['task_id']['value']
        if task_id in self.subscription.tasks:
            raise ExecutorException("Task Exists")
        self.subscription.tasks[task_id] = task_info

        log.debug("Launching %s", event)
        self.executor.on_launch(self, task_info)
        log.debug("Got launch command {}".format(event))

    def on_kill(self, event):
        task_id = event['task_id']
        self.executor.on_kill(self, task_id)
        log.debug("Got kill command {}".format(event))

    def on_acknowledged(self, event):
        task_id = event['task_id']['value']
        uuid_ = uuid.UUID(bytes=decode_data(event['uuid']))
        self.subscription.updates.pop(uuid_, None)
        self.subscription.tasks.pop(task_id, None)
        self.executor.on_acknowledged(self, task_id,uuid_)
        log.debug("Got acknowledge {}".format(event))

    def on_message(self, event):
        data = event['data']
        self.executor.on_message(self, data)

    def on_error(self, event):
        message = event['message']
        self.executor.on_error(self, message)
        log.debug("Got error {}".format(event))

    def on_shutdown(self):
        if not self.local:# pragma: no cover
            self._delay_kill()
        self.executor.on_shutdown(self)
        log.debug("Got Shutdown command")
        self.stop()

    def on_outbound_success(self, event):
        self.executor.on_outbound_success(self, event["request"])
        log.debug("Got success on outbound %s" % event)

    def on_outbound_error(self, event):
        self.executor.on_outbound_error(self, event["request"], event["endpoint"], event["error"])
        log.debug("Got error on outbound %s" % event)

    def _delay_kill(self):# pragma: no cover
        def _():
            try:
                time.sleep(self.grace_shutdown_period)
                os._exit(os.EX_OK)
            except Exception:
                log.exception('Failed to force kill executor')

        t = Thread(target=_)
        t.daemon = True
        t.start()

    def __str__(self):
        return '<%s: executor="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.subscription.master_info.info, self.framework)

    __repr__ = __str__
