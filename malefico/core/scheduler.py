from __future__ import print_function, division, absolute_import

import logging
import socket

import six
from tornado import gen
from tornado.escape import json_decode as decode
from tornado.escape import json_encode as encode
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from zoonado import Zoonado
from zoonado.exc import *

from malefico.core.connection import MesosConnection
from malefico.core.utils import log_errors, get_master, master_info, get_http_master_url

log = logging.getLogger(__name__)


class SchedulerDriver(MesosConnection):
    def __init__(self, master, scheduler, name, user='', failover_timeout=100, capabilities=[],
                 implicit_acknowledgements=True, loop=None):

        super(SchedulerDriver, self).__init__(loop=loop)
        self.master = master
        self.subscription_info = None

        self.scheduler = scheduler
        self.framework = {
            "user": user,
            "name": name,
            "capabilities": capabilities,
            "failover_timeout": failover_timeout,
            "hostname": socket.gethostname()
        }

        self.implicit_acknowledgements = implicit_acknowledgements

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

    def gen_request(self, handler):
        message = encode({
            'type': 'SUBSCRIBE',
            'subscribe': {
                'framework_info': self.framework
            }
        })
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
            'connection': 'close',
            'content-length': len(message)
        }

        subscription_r = HTTPRequest(url=self.leading_master + "/api/v1/scheduler",
                                     method='POST',
                                     headers=headers,
                                     body=message,
                                     streaming_callback=self._handlechunks,
                                     header_callback=handler,
                                     follow_redirects=False,
                                     request_timeout=1e100)
        return subscription_r

    def on_error(self, event):
        message = event['message']
        self.scheduler.on_error(self, message)

    def on_heartbeat(self, event):
        log.debug("Got heartbeat")
        message = "Heartbeat"
        self.scheduler.on_heartbeat(self, message)

    def on_subscribed(self, info):

        if self.framework_id:
            self.scheduler.on_reregistered(self, self.framework_id, self.leading_master)
        else:
            self.framework_id = info["subscribed"]['framework_id']["value"]
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
        pass

    def on_rescind(self, event):
        offer_id = event['offer_id']
        self.scheduler.on_rescinded(self, offer_id)

    def on_update(self, event):
        status = event['status']
        self.scheduler.statusUpdate(self, status)
        if self.implicit_acknowledgements:
            self.acknowledgeStatusUpdate(status)

    def on_message(self, message):
        executor_id = message['executor_id']
        agent_id = message['agent_id']
        data = message['data']
        self.scheduler.frameworkMessage(
            self, executor_id, agent_id, data
        )

    def on_failure(self, failure):
        agent_id = failure['agent_id']
        if 'executor_id' not in failure:
            self.scheduler.slaveLost(self, agent_id)
        else:
            self.scheduler.executorLost(
                self, failure['executor_id'],
                agent_id, failure['status']
            )

    @gen.coroutine
    def _detect_master(self, timeout=1):
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
                                log.warn("Master did not change, maybe just starting up, will watch for changes")
                                yield gen.sleep(timeout)
                            elif seq == -1:
                                log.warn("No master detected, will watch for changes")
                                self.leading_master = None
                                yield gen.sleep(timeout)
                            elif self.status in ("disconnected", "closed"):
                                log.warn("New master detected at %s" % self.leading_master)
                                self.leading_master_seq = seq
                                try:
                                    data = yield self.detector.get_data('/mesos/' + seq)
                                    self.leading_master_master_info = decode(data)
                                    self.leading_master = get_http_master_url(self.leading_master_master_info)
                                except NoNode as n:
                                    log.warn("Problem fetching Master node from zookeeper")

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
                    # Two implementations are possible follow the 307 or do not, following has the advantage of getting more info
                    def get_actual_master(response):
                        if response.code == 307:
                            actual_master = six.moves.urllib.parse.urlparse(response.headers["location"])
                            self.leading_master_master_info = master_info(actual_master.netloc)
                            self.leading_master = get_http_master_url(self.leading_master_master_info)
                            log.warn("New master detected at %s" % self.leading_master)
                        else:
                            self.leading_master_master_info = master_info(self.master)
                            self.leading_master = get_http_master_url(self.leading_master_master_info)
                            log.warn("New master detected at %s" % self.leading_master)

                    potential_master = get_http_master_url(master_info(self.master))
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
                    log.warn("Problem detecting master from url. Some problem with the network perhaps?")
            except Exception as ex:
                log.error("Problem resolving master:")
                log.exception(ex)

    def __str__(self):
        return '<%s: scheduler="%s:%s:%s">' % (
            self.__class__.__name__, self.master,
            self.leading_master, self.framework)

    __repr__ = __str__
