from __future__ import print_function, division, absolute_import

import logging
import uuid
from  os import environ as env

from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
from tornado.httpclient import HTTPRequest

from malefico.core.connection import MesosConnection
from malefico.core.utils import parse_duration

log = logging.getLogger(__name__)


class MesosExecutorDriver(MesosConnection):
    def __init__(self, executor, loop=None):

        self.agent_endpoint = env['MESOS_AGENT_ENDPOINT']
        super(MesosExecutorDriver, self).__init__(leading_master=self.agent_endpoint, loop=loop)

        self.framework_id = dict(value=env['MESOS_FRAMEWORK_ID'])
        assert self.framework_id
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

        self._handlers = {
            "SUBSCRIBED": self.on_subscribed,
            "MESSAGE": self.on_message,
            "LAUNCH": self.on_launch,
            "LAUNCH_GROUP": self.on_launch_group,
            "KILL": self.on_kill,
            "ACKNOWLEDGED": self.on_acknowledged,
            "SHUTDOWN": self.on_shutdown,
            "ERROR": self.on_error,
            "CLOSE": self.on_close
        }

    def gen_request(self, handler):
        message = encode({
            'type': 'SUBSCRIBE',
            'framework_id': self.framework_id,
            'executor_id': self.executor_id,
            'subscribe': {
                'unacknowledged_tasks': list(self.tasks.values()),
                'unacknowledged_updates': list(self.updates.values()),
            }
        })
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
            'connection': 'close',
            'content-length': len(message)
        }

        subscription_r = HTTPRequest(url=self.leading_master + "/api/v1/executor",
                                     method='POST',
                                     headers=headers,
                                     body=message,
                                     streaming_callback=self._handlechunks,
                                     header_callback=handler,
                                     follow_redirects=False,
                                     request_timeout=1e100)
        return subscription_r

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
                self, self._dict_cls(executor_info),
                self._dict_cls(framework_info), self._dict_cls(agent_info)
            )
        else:
            self.executor.on_reregistered(self, self._dict_cls(agent_info))

    def on_close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.version = None

        self.executor.disconnected(self)
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
        self.executor.on_launch(self, self._dict_cls(task_info))

    def on_launch(self, event):
        task_info = event['task']
        task_id = task_info['task_id']['value']
        assert task_id not in self.tasks
        self.tasks[task_id] = task_info
        self.executor.on_launch(self, self._dict_cls(task_info))

    def on_kill(self, event):
        task_id = event['task_id']
        self.executor.on_kill(self, self._dict_cls(task_id))

    def on_acknowledged(self, event):
        task_id = event['task_id']['value']
        uuid_ = uuid.UUID(bytes=decode(event['uuid']))
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

    def __str__(self):
        return '<%s: executor="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.agent_endpoint)

    __repr__ = __str__
