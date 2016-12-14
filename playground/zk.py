import asyncio
from aiozk import ZKClient
from aiozk.exc import ZKError
import logging as log


class MasterDetector(object):

    def __init__(self, uri):
        self.uri = uri
        self.zk = ZKClient(uri)
        self.masterSeq = None

    async def choose(self, children):
        children = [child for child in children if child != 'log_replicas']
        if not children:
            return True
        print("New master")
        masterSeq = min(children)
        if masterSeq == self.masterSeq:
            return True
        self.masterSeq = masterSeq
        watcher = self.zk.recipes.DataWatcher()
        watcher.add_callback('/mesos/' + masterSeq, self.notify)
        data = await watcher.fetch('/mesos/' + masterSeq)
        await self.notify(data)
        return True

    async def notify(self, master_addr):

        print(master_addr)
        return False

    async def start(self):
        await self.zk.start()
        try:
            watcher = self.zk.recipes.ChildrenWatcher()
            watcher.add_callback('/mesos', self.choose)
            children = await watcher.fetch("/mesos")
            await self.choose(children)

        except ZKError:
            await self.stop()

    async def stop(self):
        try:
            await self.zk.close
        except:
            pass

async def run():
    detector = MasterDetector("localhost")
    await detector.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    asyncio.ensure_future(run(), loop=loop)
    # asyncio.ensure_future(death(), loop=loop)
    loop.run_forever()
    loop.close()
