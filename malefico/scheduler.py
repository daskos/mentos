from __future__ import absolute_import, division, print_function

import getpass
import logging
import os
import socket

import six
from threading import Thread
from time import sleep
from tornado.ioloop import IOLoop, PeriodicCallback
from malefico.subscription import Subscription
from malefico.utils import encode_data,log_errors
from tornado import gen
from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from zoonado import Zoonado
from zoonado.exc import ConnectionLoss, NoNode

log = logging.getLogger(__name__)


class SchedulerDriver(object):

    def __init__(self, scheduler, name, user=getpass.getuser(), master=os.getenv('MESOS_MASTER') or "localhost",
                 failover_timeout=100, capabilities=[],
                 implicit_acknowledgements=True, loop=None):

        self.loop = loop or IOLoop()

        self.master = master
        self.leading_master_seq = None
        self.leading_master_info = None

        self.scheduler = scheduler
        self.framework = {
            "user": user,
            "name": name,
            "capabilities": capabilities,
            "failover_timeout": failover_timeout,
            "hostname": socket.gethostname()
        }

        self.implicit_acknowledgements = implicit_acknowledgements

        self.outbound_connection = AsyncHTTPClient(self.loop)

        self._handlers = {
            "SUBSCRIBED": self.on_subscribed,
            "OFFERS": self.on_offers,
            "RESCIND": self.on_rescind,
            "UPDATE": self.on_update,
            "MESSAGE": self.on_message,
            "RESCIND_INVERSE_OFFER": self.on_rescind_inverse,
            "FAILURE": self.on_failure,
            "ERROR": self.on_error,
            "HEARTBEAT": self.on_heartbeat
        }
        self.session = Subscription(self.gen_request,self._handle_events,master=master,endpoint="/api/v1/scheduler",timeout=failover_timeout)


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

        self.loop.add_callback(self.session.start, detector=True)
        if block:
            self._loop_thread.join()

    def _handle_outbound(self, response):
        if response.code not in (200, 202):
            log.error("Problem with request to  Master for payload %s" %
                      response.request.body)
            log.error(response.body)
            self.scheduler.on_outbound_error(self, response)
        else:
            self.scheduler.on_outbound_success(self, response)
            log.debug("Succeed request to master %s" %
                      response.request.body)


    def _send(self, payload):

        data = encode(payload)
        headers = {
            'Content-Type': 'application/json'
        }
        if self.mesos_stream_id:
            headers['Mesos-Stream-Id'] = self.mesos_stream_id

        self.outbound_connection.fetch(
            HTTPRequest(
                url=self.leading_master + "/api/v1/scheduler",
                body=data,
                method='POST',
                headers=headers,
            ), self._handle_outbound
        )

    def request(self, requests):
        """
        """
        payload = {
            "framework_id": self.framework_id,
            "type": "REQUEST",
            "requests": requests
        }
        self.loop.add_callback(self._send, payload)
        log.warn('Request resources from Mesos')

    def kill(self, task_id, agent_id):
        """
        """
        payload = {
            "framework_id": self.framework_id,
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
        self.loop.add_callback(self._send, payload)
        log.warn('Kills task {}'.format(task_id))

    def reconcile(self, task_id, agent_id):
        """
        """
        if task_id and agent_id:
            payload = {
                 "framework_id": self.framework_id,
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
                "framework_id": self.framework_id,
                "type": "RECONCILE",
                "reconcile": {"tasks": []}
            }
            log.warn("Reconciling all tasks ")
        if payload:
            self.loop.add_callback(self._send, payload)
        else:
            log.warn("Agent and Task not set")

    def decline(self, offer_ids, filters=None):
        """
        """
        decline = {
            "offer_ids": [offer_ids] if isinstance(offer_ids, dict) else offer_ids
        }

        if filters is not None:
            decline['filters'] = filters

        payload = {
            "framework_id": self.framework_id,
            "type": "DECLINE",
            "decline": decline
        }
        self.loop.add_callback(self._send, payload)
        log.warn('Declines offer {}'.format(offer_ids))

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

    def accept(self, offer_ids, operations, filters=None):
        """
        """
        if not operations:
            return self.decline(offer_ids, filters=filters)

        accept = {
            "offer_ids": offer_ids,
            "operations": operations
        }

        if filters is not None:
            accept['filters'] = filters

        payload = {
            "framework_id": self.framework_id,
            "type": "ACCEPT",
            "accept": accept
        }
        self.loop.add_callback(self._send, payload)
        log.warn('Accepts offers {}'.format(offer_ids))

    def revive(self):
        """
        """
        payload = {
             "framework_id": self.framework_id,
            "type": "REVIVE"
        }
        self.loop.add_callback(self._send, payload)
        log.warn(
            'Revives; removes all filters previously set by framework')

    def acknowledge(self, status):
        """
        """
        if 'uuid' not in status:
            log.debug(
                "Did not get a UUID for %s" % status)
            return

        payload = {
            "framework_id": self.framework_id,
            "type": "ACKNOWLEDGE",
            "acknowledge": {
                "agent_id": status["agent_id"],
                "task_id": status["task_id"],
                "uuid": status["uuid"]
            }
        }
        self.loop.add_callback(self._send, payload)
        log.warn('Acknowledges status update {}'.format(status))

    def message(self, executor_id, agent_id, message):
        """
        """
        payload = {
            "framework_id": self.framework_id,
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
        self.loop.add_callback(self._send, payload)
        log.warn('Sends message `{}` to executor `{}` on agent `{}`'.format(
            message, executor_id, agent_id))

    def shutdown(self, agent_id, executor_Id):
        """
        """
        payload = {
            "framework_id": self.framework_id,
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
        self.loop.add_callback(self._send, payload)
        log.warn("Sent shutdown signal")

    def teardown(self, framework_id):
        """
        """
        payload = {
            "framework_id": self.framework_id,
            "type": "TEARDOWN"
        }

        self.loop.add_callback(self._send, payload)
        log.warn("Sent teardown signal")

    def on_error(self, event):
        message = event['message']
        self.scheduler.on_error(self, message)

    def on_heartbeat(self, event):
        log.debug("Got heartbeat")
        message = "Heartbeat"
        self.scheduler.on_heartbeat(self, message)

    def on_subscribed(self, info):

        if self.framework_id:
            self.scheduler.on_reregistered(
                self, self.framework_id, self.leading_master)
        else:
            self.framework_id = info["framework_id"]
            self.framework['framework_id'] = self.framework_id
            self.scheduler.on_registered(
                self, self.framework_id,
                self.leading_master
            )

    def on_offers(self, event):
        offers = event['offers']
        self.scheduler.on_offers(
            self, offers
        )

    def on_rescind_inverse(self, event):
        offer_id = event['offer_id']
        self.scheduler.on_rescind_inverse(self, offer_id)

    def on_rescind(self, event):
        offer_id = event['offer_id']
        self.scheduler.on_rescinded(self, offer_id)

    def on_update(self, event):
        status = event['status']
        self.scheduler.on_update(self, status)
        if self.implicit_acknowledgements:
            self.acknowledge(status)

    def on_message(self, event):
        executor_id = event['executor_id']
        agent_id = event['agent_id']
        data = event['data']
        self.scheduler.on_message(
            self, executor_id, agent_id, data
        )

    def on_failure(self, event):
        agent_id = event['agent_id']
        if 'executor_id' not in event:
            self.scheduler.on_agent_lost(self, agent_id)
        else:
            executor_id = event['executor_id']
            status = event['status']
            self.scheduler.on_executor_lost(
                self, executor_id,
                agent_id, status
            )

    @gen.coroutine
    def _handle_events(self, message):
        with log_errors():
            try:
                if message["type"] in self._handlers:
                    _type = message['type']
                    log.warn("Got event of type %s" % _type)
                    if _type == "HEARTBEAT":
                        self._handlers[_type](message)
                    else:
                        self._handlers[_type](message[_type.lower()])

                else:
                    log.warn("Unhandled event %s" % message)
            except Exception as ex:
                log.warn("Problem dispatching event %s" % message)
                log.exception(ex)

    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.leading_master, self.framework)

    __repr__ = __str__

    def __enter__(self):
        if not self.loop._running:
            self.start(block=False)
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
