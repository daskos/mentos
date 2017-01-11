from __future__ import absolute_import, division, print_function

import getpass
import logging
import os
import socket
from threading import Thread
from time import sleep

from toolz import merge
from tornado import gen
from tornado.ioloop import IOLoop

from malefico.subscription import Subscription, Event
from malefico.utils import encode_data

log = logging.getLogger(__name__)


class SchedulerDriver(object):
    def __init__(self, scheduler, name, user=getpass.getuser(), master=os.getenv('MESOS_MASTER') or "localhost",
                 failover_timeout=100, capabilities=None,
                 implicit_acknowledgements=True, handlers=None, loop=None):

        self.loop = loop or IOLoop()

        self.master = master
        self.leading_master_seq = None
        self.leading_master_info = None

        self.scheduler = scheduler
        self.framework = {
            "user": user,
            "name": name,
            "capabilities": capabilities or [],
            "failover_timeout": failover_timeout,
            "hostname": socket.gethostname()
        }

        self.implicit_acknowledgements = implicit_acknowledgements

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

        self.subscription = Subscription(self.framework, self.master, "/api/v1/scheduler", self.handlers,
                                         timeout=failover_timeout,
                                         loop=self.loop)

    def start(self, block=False, **kwargs):
        """ Start scheduler running in separate thread """
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

    @gen.coroutine
    def request(self, requests):
        """
        """
        payload = {
            "type": "REQUEST",
            "requests": requests
        }
        yield self.subscription.send(payload)
        log.warn('Request resources from Mesos')


    def kill(self, task_id, agent_id):

        payload = {

            "type": "KILL",
            "kill": {
                "task_id": {
                    "value": task_id
                },
                "agent_id": {
                    "value": agent_id
                }
            }
        }
        yield self.subscription.send(payload)
        log.warn('Kills task {}'.format(task_id))

    @gen.coroutine
    def reconcile(self, task_id, agent_id):
        """
        """
        if task_id and agent_id:
            payload = {
                "type": "RECONCILE",
                "reconcile": {
                    "tasks": [{
                        "task_id": {
                            "value": task_id
                        },
                        "agent_id": {
                            "value": agent_id
                        }
                    }]
                }}

            log.warn("Reconciling task ID: " + task_id)

        else:
            payload = {

                "type": "RECONCILE",
                "reconcile": {"tasks": []}
            }
            log.warn("Reconciling all tasks ")
        if payload:
            yield self.subscription.send(payload)
        else:
            log.warn("Agent and Task not set")

    @gen.coroutine
    def decline(self, offer_ids, filters=None):
        """
        """
        decline = {
            "offer_ids": [offer_ids] if isinstance(offer_ids, dict) else offer_ids
        }

        if filters is not None:
            decline['filters'] = filters

        payload = {
            "type": "DECLINE",
            "decline": decline
        }

        yield self.subscription.send(payload)
        log.warn('Declines offer {}'.format(offer_ids))

    @gen.coroutine
    def launch(self, offer_ids, tasks, filters=None):
        if not tasks:
            return self.decline(offer_ids, filters=filters)

        operations = [{
            'type': 'LAUNCH',
            'launch': {
                'task_infos': tasks
            }
        }]
        self.accept(offer_ids, operations, filters=filters)

    @gen.coroutine
    def accept(self, offer_ids, operations, filters=None):
        """
        """
        if not operations:
            yield self.decline(offer_ids, filters=filters)
        else:
            accept = {
                "offer_ids": offer_ids,
                "operations": operations
            }

            if filters is not None:
                accept['filters'] = filters

            payload = {

                "type": "ACCEPT",
                "accept": accept
            }
            yield self.subscription.send(payload)
            log.warn('Accepts offers {}'.format(offer_ids))

    @gen.coroutine
    def revive(self):
        """
        """
        payload = {

            "type": "REVIVE"
        }
        yield self.subscription.send(payload)
        log.warn(
            'Revives; removes all filters previously set by framework')

    @gen.coroutine
    def acknowledge(self, status):
        """
        """
        if 'uuid' not in status:
            log.debug(
                "Did not get a UUID for %s" % status)
            return

        payload = {

            "type": "ACKNOWLEDGE",
            "acknowledge": {
                "agent_id": status["agent_id"],
                "task_id": status["task_id"],
                "uuid": status["uuid"]
            }
        }
        yield self.subscription.send(payload)
        log.warn('Acknowledges status update {}'.format(status))

    @gen.coroutine
    def message(self, executor_id, agent_id, message):
        """
        """
        payload = {

            "type": "MESSAGE",
            "message": {
                "agent_id": {
                    "value": agent_id
                },
                "executor_id": {
                    "value": executor_id
                },
                "data": encode_data(message)
            }
        }
        yield self.subscription.send(payload)
        log.warn('Sends message `{}` to executor `{}` on agent `{}`'.format(
            message, executor_id, agent_id))

    @gen.coroutine
    def shutdown(self, agent_id, executor_Id):
        """
        """
        payload = {

            "type": "SHUTDOWN",
            "kill": {
                "executor_id": {
                    "value": executor_Id
                },
                "agent_id": {
                    "value": agent_id
                }
            }
        }
        yield self.subscription.send(payload)
        log.warn("Sent shutdown signal")

    @gen.coroutine
    def teardown(self, framework_id):
        """
        """
        payload = {

            "type": "TEARDOWN"
        }

        yield self.subscription.send(payload)
        log.warn("Sent teardown signal")

    @gen.coroutine
    def on_error(self, event):
        message = event['message']
        yield self.scheduler.on_error(self, message)

    @gen.coroutine
    def on_heartbeat(self, event):
        yield self.scheduler.on_heartbeat(self, event)

    @gen.coroutine
    def on_subscribed(self,info):
        print(info)
        yield self.scheduler.on_reregistered(
            self, info["framework_id"], self.subscription.master_info.info)

    @gen.coroutine
    def on_offers(self, event):
        offers = event['offers']
        yield self.scheduler.on_offers(
            self, offers
        )

    @gen.coroutine
    def on_rescind_inverse(self, event):
        offer_id = event['offer_id']
        yield self.scheduler.on_rescind_inverse(self, offer_id)

    @gen.coroutine
    def on_rescind(self, event):
        offer_id = event['offer_id']
        yield self.scheduler.on_rescinded(self, offer_id)

    @gen.coroutine
    def on_update(self, event):
        status = event['status']
        yield self.scheduler.on_update(self, status)
        if self.implicit_acknowledgements:
            self.acknowledge(status)

    @gen.coroutine
    def on_message(self, event):
        executor_id = event['executor_id']
        agent_id = event['agent_id']
        data = event['data']
        yield self.scheduler.on_message(
            self, executor_id, agent_id, data
        )

    @gen.coroutine
    def on_failure(self, event):
        agent_id = event['agent_id']
        if 'executor_id' not in event:
            yield self.scheduler.on_agent_lost(self, agent_id)
        else:
            executor_id = event['executor_id']
            status = event['status']
            yield self.scheduler.on_executor_lost(
                self, executor_id,
                agent_id, status
            )

    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.subscription.master_info.info, self.framework)

    __repr__ = __str__

    def __enter__(self):
        if not self.loop._running:
            self.start(block=False)
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
