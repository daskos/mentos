from __future__ import print_function, division, absolute_import

import logging
import socket

import six
from tornado import gen
from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from zoonado import Zoonado
from zoonado.exc import ConnectionLoss, NoNode
from malefico.core.connection import MesosConnection
from malefico.core.utils import log_errors, get_master, master_info, get_http_master_url, encode_data

log = logging.getLogger(__name__)


class MesosSchedulerDriver(MesosConnection):
    def __init__(self, master, scheduler, name, user='', failover_timeout=100, capabilities=[],
                 implicit_acknowledgements=True, loop=None):

        super(MesosSchedulerDriver, self).__init__(loop=loop)
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

    @property
    def framework_id(self):
        id = self.framework.get('framework_id')
        return id and id.get('value')

    @framework_id.setter
    def framework_id(self, id):
        self.framework['framework_id'] = dict(value=id)

    # <editor-fold desc="Scheduler actions">

    def _send(self, payload):
        data = encode(payload)
        def handle_response(response):
            if response.code not in (200, 202):
                log.warn("Problem with request to master. %s" % response)

        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
            'connection': 'close',
            'content-length': len(data),
            'Mesos-Stream-Id': self.mesos_stream_id
        }

        self.outbound_connection.fetch(self.leading_master + "/api/v1/scheduler", handle_response, method='POST',
                                       headers=headers, body=data)

    def request(self, requests):
        """
        """
        payload = {
            "framework_id": {
                "value": self.framework_id
            },
            "type": "REQUEST",
            "requests": requests
        }
        self._send(payload)
        log.warn('Request resources from Mesos')

    def kill(self, task_id, agent_id):
        """
        """
        payload = {
            "framework_id": {
                "value": self.framework_id
            },
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
        self._send(payload)
        log.warn('Kills task {}'.format(task_id))

    def reconcile(self, task_id, agent_id):
        """
        """
        payload = {};
        if task_id and agent_id:
            payload = {
                "framework_id": {
                    "value": self.framework_id
                },
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
                "framework_id": {
                    "value": self.framework_id
                },
                "type": "RECONCILE",
                "reconcile": {"tasks": []}
            }
            log.warn("Reconciling all tasks ");
        if payload:
            self._send(payload)
        else:
            log.warn("Agent and Task not set")

    def decline(self, offer_ids, filters={}):
        """
        """
        decline = {
            "offer_ids": [offer_ids] if isinstance(offer_ids, dict) else offer_ids
        }

        if filters is not None:
            decline['filters'] = filters

        payload = {
            "framework_id": {
                "value": self.framework_id
            },
            "type": "DECLINE",
            "decline": decline
        }

        log.warn('Declines offer {}'.format(offer_ids))

    def launch(self, offer_ids, tasks, filters={}):
        if not tasks:
            return self.decline(offer_ids, filters=filters)

        operations = [{
            'type': 'LAUNCH',
            'launch': {
                'task_infos': tasks
            }
        }]

        self.accept(offer_ids, operations, filters=filters)

    def accept(self, offer_ids, operations, filters={}):
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
            "framework_id": {
                "value": self.framework_id
            },
            "type": "ACCEPT",
            "accept": accept
        }
        self._send(payload)
        log.warn('Accepts offers {}'.format(offer_ids))

    def revive(self):
        """
        """
        payload = {
            "framework_id": {
                "value": self.frameworkId
            },
            "type": "REVIVE"
        }
        log.warn(
            'Revives; removes all filters previously set by framework')

    def acknowledge(self, status):
        """
        """
        if 'uuid' not in status:
            self.log.debug(
                "Did not get a UUID for %s" % status);
            return;

        payload = {
            "framework_id": {
                "value": self.framework_id
            },
            "type": "ACKNOWLEDGE",
            "acknowledge": {
                "agent_id": status["agent_id"],
                "task_id": status["task_id"],
                "uuid": status["uuid"]
            }
        }
        log.warn('Acknowledges status update {}'.format(status))

    def message(self, executor_id, agent_id, message):
        """
        """
        payload = {
            "framework_id": {
                "value": self.framework_id
            },
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
        self._send(payload)
        log.warn('Sends message `{}` to executor `{}` on agent `{}`'.format(
            message, executor_id, agent_id))

    def shutdown(self, agent_id, executor_Id):
        """
        """
        payload = {
            "framework_id": {
                "value": self.framework_id
            },
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
        self._send(payload)
        log.warn("Sent shutdown signal")

    def teardown(self, framework_id):
        """
        """
        payload = {
            "framework_id": {
                "value": self.framework_id
            },
            "type": "TEARDOWN"
        }

        self._send(payload)
        log.warn("Sent teardown signal")

    # </editor-fold>

    # <editor-fold desc="Event handlers">

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
            self.framework_id = info['framework_id']["value"]
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

    def on_message(self, message):
        executor_id = message['executor_id']
        agent_id = message['agent_id']
        data = message['data']
        self.scheduler.on_message(
            self, executor_id, agent_id, data
        )

    def on_failure(self, failure):
        agent_id = failure['agent_id']
        if 'executor_id' not in failure:
            self.scheduler.on_agent_lost(self, agent_id)
        else:
            self.scheduler.on_executor_lost(
                self, failure['executor_id'],
                agent_id, failure['status']
            )

    # </editor-fold>

    @gen.coroutine
    def _handle_events(self, message):
        with log_errors():
            try:
                if message["type"] in self._handlers:
                    _type = message['type']
                    if _type == "HEARTBEAT":
                        self._handlers[_type](message)
                    else:
                        self._handlers[_type](message[_type.lower()])

                else:
                    log.warn("Unhandled event %s" % message)
            except Exception as ex:
                log.warn("Problem dispatching event %s" % message)
                log.exception(ex)

    def gen_request(self, handler):
        payload = encode({
            'type': 'SUBSCRIBE',
            'subscribe': {
                'framework_info': self.framework
            }
        })
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
            'connection': 'close',
            'content-length': len(payload)
        }

        subscription_r = HTTPRequest(url=self.leading_master + "/api/v1/scheduler",
                                     method='POST',
                                     headers=headers,
                                     body=payload,
                                     streaming_callback=self._handlechunks,
                                     header_callback=handler,
                                     follow_redirects=False,
                                     request_timeout=1e100)
        return subscription_r

    @gen.coroutine
    def _detect_master(self, timeout=5):
        with log_errors():
            try:
                if "zk://" in self.master:
                    log.warn("Using Zookeeper for discovery")
                    quorum = ",".join([zoo[zoo.index('://') + 3:]
                                       for zoo in self.master.split(",")])
                    self.detector = Zoonado(quorum)
                    try:
                        yield self.detector.start()

                        @gen.coroutine
                        def children_changed(children):
                            yield gen.sleep(timeout)
                            current_state = yield self.detector.get_children("/mesos")
                            seq = get_master(current_state)
                            if seq == self.leading_master_seq and self.status == "disconnected":
                                log.warn(
                                    "Master did not change, maybe just starting up, will watch for changes")
                                yield gen.sleep(timeout)
                            elif seq == -1:
                                log.warn(
                                    "No master detected, will watch for changes")
                                log.warn("Waiting for %d" % timeout)
                                self.leading_master = None
                                yield gen.sleep(timeout)
                            elif self.status in ("disconnected", "closed"):
                                log.warn("New master detected at %s" %
                                         self.leading_master)
                                self.leading_master_seq = seq
                                try:
                                    data = yield self.detector.get_data('/mesos/' + seq)
                                    self.leading_master_info = decode(
                                        data)
                                    self.leading_master = get_http_master_url(
                                        self.leading_master_info)
                                except NoNode as n:
                                    log.warn(
                                        "Problem fetching Master node from zookeeper")

                        watcher = self.detector.recipes.ChildrenWatcher()
                        watcher.add_callback(
                            '/mesos', children_changed)
                        children = yield self.detector.get_children('/mesos')
                        yield children_changed(children)
                    except ConnectionLoss as n:
                        pass
                    except Exception as ex:
                        log.error("Unhandled exception in Detector")
                        yield self.detector.close()
                else:
                    # Two implementations are possible follow the 307 or do
                    # not, following has the advantage of getting more info
                    def get_actual_master(response):
                        if response.code == 307:
                            actual_master = six.moves.urllib.parse.urlparse(
                                response.headers["location"])
                            self.leading_master_info = master_info(
                                actual_master.netloc)
                            self.leading_master = get_http_master_url(
                                self.leading_master_info)
                            log.warn("New master detected at %s" %
                                     self.leading_master)
                        elif response.code in (200, 202):
                            self.leading_master_info = master_info(
                                self.master)
                            self.leading_master = get_http_master_url(
                                self.leading_master_info)
                            log.warn("New master detected at %s" %
                                     self.leading_master)

                    potential_master = get_http_master_url(
                        master_info(self.master))
                    check_master_r = HTTPRequest(url=potential_master + "/state",
                                                 method='GET',
                                                 headers={
                                                     'content-type': 'application/json',
                                                     'accept': 'application/json',
                                                     'connection': 'close',
                                                 },
                                                 follow_redirects=False)

                    http_client = AsyncHTTPClient()
                    yield http_client.fetch(check_master_r, get_actual_master)

            except HTTPError as ex:
                if ex.code == "307":
                    pass
                else:
                    log.warn(
                        "Problem resolving Master. Will retry.")
                    log.warn("Waiting for %d" % timeout)
                    # TODO Backoff
                    yield gen.sleep(timeout)
                    yield self._detect_master(timeout)
            except Exception as ex:
                log.error("Unhandeled exception")
                log.exception(ex)



    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.leading_master, self.framework)

    __repr__ = __str__
