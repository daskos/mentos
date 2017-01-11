from __future__ import absolute_import, division, print_function

import logging
import os
from os import environ as env
import signal
from threading import Thread
import time
import uuid

from malefico.subscription import Subscription,Event
from malefico.utils import decode_data, encode_data, log_errors, parse_duration
from tornado import gen
from toolz import merge
log = logging.getLogger(__name__)


class ExecutorDriver(Subscriber):

    def __init__(self, executor,  handlers=None, loop=None):
        """

        Args:
            executor ():
            loop ():
        """

        self.master = env['MESOS_AGENT_ENDPOINT']

        self.framework_id = dict(value=env['MESOS_FRAMEWORK_ID'])
        self.executor_id = dict(value=env['MESOS_EXECUTOR_ID'])

        grace_shutdown_period = env.get('MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD')
        if grace_shutdown_period:
            self.grace_shutdown_period = parse_duration(grace_shutdown_period)
        else:
            self.grace_shutdown_period = 0.0

        self.checkpoint = bool(env.get('MESOS_CHECKPOINT'))
        self.local = bool(env.get('MESOS_LOCAL'))

        self.executor = executor
        self.framework_info = None
        self.executor_info = None
        self.tasks = {}
        self.updates = {}

        self.executor = executor
        self.handlers = merge({
            Event.SUBSCRIBED: self.on_subscribed,
            Event.OFFERS: self.on_offers,
            Event.RESCIND: self.on_rescind,
            Event.UPDATE: self.on_update,
            Event.MESSAGE: self.on_message,
            Event.RESCIND_INVERSE_OFFER: self.on_rescind_inverse,
            Event.FAILURE: self.on_failure,
            Event.ERROR: self.on_error,
            Event.HEARTBEAT: self.on_heartbeat
        }, handlers or {})

        'type': 'SUBSCRIBE',
        'framework_id': self.framework_id,
        'executor_id': self.executor_id,
        'subscribe': {
            'unacknowledged_tasks': list(self.tasks.values()),
            'unacknowledged_updates': list(self.updates.values()),
        }

        self.subscription = Subscription(self.framework, self.master, "/api/v1/executor", self.handlers,
                                     loop=self.loop)

    def start(self, block=False, **kwargs):
        """ Start executor running in separate thread """
        if hasattr(self, '_loop_thread'):
            return
        if not self.loop._running:

            self._loop_thread = Thread(target=self.loop.start)
            self._loop_thread.daemon = True
            self._loop_thread.start()
            while not self.loop._running:
                sleep(0.001)

        self.loop.add_callback(self.subscription.start)
        if block:
            self._loop_thread.join()

    def stop(self):
        raise NotImplementedError

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
        self.loop.add_callback(self._send, payload)
        logging.info('Executor sends status update {} for task {}'.format(
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
        self.loop.add_callback(self._send, payload)
        logging.info('Driver sends framework message {}'.format(message))

    def on_subscribed(self, info):
        executor_info = info['executor_info']
        framework_info = info['framework_info']
        agent_info = info['agent_info']
        assert executor_info['executor_id'] == self.executor_id
        assert framework_info['id'] == self.framework_id

        if self.executor_info is None or self.framework_info is None:
            self.executor_info = executor_info
            self.framework_info = framework_info
            self.executor.on_registered(
                self, executor_info,
                self.framework_info, agent_info
            )
        else:
            self.executor.on_reregistered(self, agent_info)

    def on_close(self):
        if not self.checkpoint:
            if not self.local:
                self._delay_kill()
            self.executor.shutdown(self)
            self.abort()

    def on_launch_group(self, event):
        task_info = event['task']
        task_id = task_info['task_id']['value']
        assert task_id not in self.tasks
        self.tasks[task_id] = task_info
        self.executor.on_launch(self, task_info)

    def on_launch(self, event):
        task_info = event['task']
        task_id = task_info['task_id']['value']
        assert task_id not in self.tasks
        self.tasks[task_id] = task_info
        self.executor.on_launch(self, task_info)

    def on_kill(self, event):
        task_id = event['task_id']
        self.executor.on_kill(self, task_id)

    def on_acknowledged(self, event):
        task_id = event['task_id']['value']
        uuid_ = uuid.UUID(bytes=decode_data(event['uuid']))
        self.updates.pop(uuid_, None)
        self.tasks.pop(task_id, None)

    def on_message(self, event):
        data = event['data']
        self.executor.on_message(self, data)

    def on_error(self, event):
        message = event['message']
        self.executor.on_error(self, message)

    def on_shutdown(self):
        if not self.local:
            self._delay_kill()
        self.executor.on_shutdown(self)
        self.stop()


    def _delay_kill(self):
        def _():
            try:
                time.sleep(self.grace_shutdown_period)
                os.killpg(0, signal.SIGKILL)
            except Exception:
                log.exception('Failed to force kill executor')

        t = Thread(target=_)
        t.daemon = True
        t.start()

    def __str__(self):
        return '<%s: executor="%s:%s:%s">' % (
            self.__class__.__name__, self.master, self.framework_id,
            self.agent_endpoint)

    __repr__ = __str__
