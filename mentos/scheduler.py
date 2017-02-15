from __future__ import absolute_import, division, print_function

import getpass
import logging
import os
import socket
from threading import Thread
from time import sleep

from mentos.subscription import Event, Subscription
from mentos.utils import encode_data,run_background
from toolz import merge
from tornado.ioloop import IOLoop

log = logging.getLogger(__name__)


class SchedulerDriver(object):

    def __init__(self, scheduler, name, user=getpass.getuser(),
                 master=os.getenv('MESOS_MASTER', 'zk://localhost:2181'),
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

        self.handlers = merge({Event.SUBSCRIBED: self.on_subscribed,
                            Event.OFFERS: self.on_offers,
                            Event.RESCIND: self.on_rescind,
                            Event.UPDATE: self.on_update,
                            Event.MESSAGE: self.on_message,
                            Event.RESCIND_INVERSE_OFFER: self.on_rescind_inverse,
                            Event.FAILURE: self.on_failure,
                            Event.ERROR: self.on_error,
                            Event.HEARTBEAT: self.on_heartbeat,
                            Event.OUTBOUND_SUCCESS: self.on_outbound_success,
                            Event.OUTBOUND_ERROR: self.on_outbound_error}, handlers or {})

        self.subscription = Subscription(self.framework, self.master,
                                         "/api/v1/scheduler", self.handlers,
                                         timeout=failover_timeout,
                                         loop=self.loop)

    def start(self, block=False, **kwargs):
        """ Start scheduler running in separate thread """
        log.debug("Starting scheduler")
        # if hasattr(self, '_loop_thread'):
        #     if not self._loop_thread._is_stopped:
        #         return
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
        if self.subscription:
            self.subscription.close()
        if self.loop:
            def on_complete(self):
                log.debug("Closed scheduler")

            run_background(self.loop.stop, on_complete)
            #self.loop.add_callback(sp)
            while self.loop._running:
                sleep(0.1)

    def request(self, requests):
        """
        """
        payload = {
            "type": "REQUEST",
            "requests": requests
        }
        self.loop.add_callback(self.subscription.send, payload)
        log.debug('Request resources from Mesos')

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
        self.loop.add_callback(self.subscription.send, payload)
        log.debug('Kills task {}'.format(task_id))

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
            log.debug("Reconciling all tasks ")
        if payload:
            self.loop.add_callback(self.subscription.send, payload)
        else:# pragma: no cover
            log.debug("Agent and Task not set")

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

        self.loop.add_callback(self.subscription.send, payload)
        log.debug('Declines offer {}'.format(offer_ids))

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


        log.debug('Launching operations {} with filters {}'.format(
            operations, filters))


    def accept(self, offer_ids, operations, filters=None):
        """
        """
        if not operations:
            self.decline(offer_ids, filters=filters)
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

            self.loop.add_callback(self.subscription.send, payload)

            log.debug('Accepts offers {}'.format(offer_ids))

    def revive(self):
        """
        """
        payload = {

            "type": "REVIVE"
        }
        self.loop.add_callback(self.subscription.send, payload)
        log.debug(
            'Revives; removes all filters previously set by framework')

    def acknowledge(self, status):
        """
        """
        if 'uuid' not in status:
            log.warn(
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
        self.loop.add_callback(self.subscription.send, payload)
        log.debug('Acknowledges status update {}'.format(status))

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
        self.loop.add_callback(self.subscription.send, payload)
        log.debug('Sends message `{}` to executor `{}` on agent `{}`'.format(
            message, executor_id, agent_id))

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
        self.loop.add_callback(self.subscription.send, payload)
        log.debug("Sent shutdown signal")

    def teardown(self, framework_id):
        """
        """
        payload = {

            "type": "TEARDOWN"
        }

        self.loop.add_callback(self.subscription.send, payload)
        log.debug("Sent teardown signal")

    def on_error(self, event):
        message = event['message']
        self.scheduler.on_error(self, message)
        log.debug("Got error %s" % event)

    def on_heartbeat(self, event):
        self.scheduler.on_heartbeat(self, event)
        log.debug("Got Heartbeat")

    def on_subscribed(self, info):
        self.scheduler.on_reregistered(
            self, info["framework_id"], self.subscription.master_info.info)

        log.debug("Subscribed %s" % info)

    def on_offers(self, event):
        offers = event['offers']
        self.scheduler.on_offers(
            self, offers
        )
        log.debug("Got offers %s" % event)

    def on_rescind_inverse(self, event):
        offer_id = event['offer_id']
        self.scheduler.on_rescind_inverse(self, offer_id)
        log.debug("Inverse rescind offer %s" % event)

    def on_rescind(self, event):
        offer_id = event['offer_id']
        self.scheduler.on_rescinded(self, offer_id)
        log.debug("Rescind offer %s" % event)

    def on_update(self, event):
        status = event['status']
        self.scheduler.on_update(self, status)
        if self.implicit_acknowledgements:
            self.acknowledge(status)
        log.debug("Got update %s" % event)

    def on_message(self, event):
        executor_id = event['executor_id']
        agent_id = event['agent_id']
        data = event['data']
        self.scheduler.on_message(
            self, executor_id, agent_id, data
        )
        log.debug("Got message %s" % event)

    def on_failure(self, event):
        agent_id = event['agent_id']
        if 'executor_id' not in event:
            self.scheduler.on_agent_lost(self, agent_id)
            log.debug("Lost agent %s" % agent_id)
        else:
            executor_id = event['executor_id']
            status = event['status']
            self.scheduler.on_executor_lost(
                self, executor_id,
                agent_id, status
            )
            log.debug("Lost executor %s on agent %s" % (executor_id, agent_id))

    def on_outbound_success(self, event):
        self.scheduler.on_outbound_success(self, event["request"])
        log.debug("Got success on outbound %s" % event)

    def on_outbound_error(self, event):
        self.scheduler.on_outbound_error(self, event["request"],event["endpoint"],event["error"])
        log.debug("Got error on outbound %s" % event)

    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.subscription.master_info.info, self.framework)

    __repr__ = __str__

    def __enter__(self):
        if not self.loop._running:
            log.debug("Entering context manager")
            self.start(block=False)
        return self

    def __exit__(self, type, value, traceback):
        log.debug("Exited context manager")
        self.stop()

    def __del__(self):# pragma: no cover
        log.debug("Deleting scheduler")
        self.stop()
