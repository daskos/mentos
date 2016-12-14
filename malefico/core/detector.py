import asyncio
from tornado import gen
from zoonado import Zoonado
from zoonado.exc import  ZKError
from malefico.core.utils import get_master

class MasterDetector(object):

    def __init__(self, quorum,scheduler):
        self.url = quorum
        self.zk = Zoonado(quorum)
        self.master_seq = None
        self.scheduler = scheduler

    def choose(self, children):
        master_seq = get_master(children)
        if master_seq == self.master_seq:
            return True
        self.master_seq = master_seq
        watcher = self.zk.recipes.DataWatcher()
        print(master_seq)
        watcher.add_callback('/mesos/' + master_seq, self.notify)
        # data = yield watcher.fetch('/mesos/' + master_seq)
        # yield self.notify(data)
        return True

    def notify(self, master_addr):
        self.scheduler.emit("master_changed",master_addr)
        print(master_addr)
        return False

    @gen.coroutine
    def start(self):
        yield self.zk.start()
        try:
            watcher = self.zk.recipes.ChildrenWatcher()
            watcher.add_callback('/mesos', self.choose)
            children = yield watcher.fetch("/mesos")
            #yield self.choose(children)

        except ZKError:
            yield self.stop()

    @gen.coroutine
    def stop(self):
        try:
            yield self.zk.close()
        except:
            pass

