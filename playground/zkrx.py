from __future__ import print_function

from kazoo.client import KazooClient as ZKClient
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.exceptions import ZookeeperError


def adjust_zk_logging_level():
    import logging
    import kazoo
    kazoo.client.log.setLevel(logging.WARNING)
    kazoo.protocol.connection.log.setLevel(logging.WARNING)


class MasterDetector(object):

    def __init__(self, uri, subject):
        self.uri = uri
        self.zk = ZKClient(uri, 10)
        self.subject = subject
        self.masterSeq = None

    def choose(self, children):
        children = [child for child in children if child != 'log_replicas']
        if not children:

            return True
        masterSeq = min(children)
        if masterSeq == self.masterSeq:
            return True
        self.masterSeq = masterSeq
        DataWatch(self.zk, '/mesos/' + masterSeq, self.notify)
        return True

    def notify(self, master_addr, _):
        self.subject.on_next(master_addr)
        return False

    def start(self):
        adjust_zk_logging_level()
        self.zk.start()
        try:
            ChildrenWatch(self.zk, 'mesos', self.choose)
        except ZookeeperError:
            self.stop()

    def stop(self):
        try:
            self.zk.stop()
        except:
            pass
