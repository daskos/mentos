
from typing import Generic, TypeVar, Iterable
import asyncio
import logging
import aiohttp
from aioreactive.core import AsyncSink, AsyncSource
from aioreactive.core import AsyncSingleStream
import json
log = logging.getLogger(__name__)
T = TypeVar('T')


class MesosSubscripion(AsyncSource, Generic[T]):

    def __init__(self, url, subscription) -> None:
        self.url = url
        self.subscription = subscription

    async def __astart__(self, sink: AsyncSink) -> AsyncSingleStream:

        def cancel(sub):
            self.session.close()

        sub = AsyncSingleStream()
        sub.add_done_callback(cancel)

        async def async_worker() -> None:
            try:
                async with aiohttp.ClientSession(headers={'content-type': 'application/json'}) as session:
                    async with session.post(self.url, data=json.dumps(self.subscription),
                                            timeout=100000) as response:
                        async for value in response.content:
                            await sink.asend(value)

            except Exception as ex:
                await sink.athrow(ex)
                return
            await sink.aclose()
        try:
            task = asyncio.ensure_future(async_worker())
        except Exception as ex:
            log.debug("MesosSubscripion:worker(), Exception: %s" % ex)
            await sink.athrow(ex)
        return sub


def mesos_sbuscription(url, msg) -> AsyncSource[T]:

    return MesosSubscripion(url, msg)
