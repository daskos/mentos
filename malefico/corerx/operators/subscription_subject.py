
from typing import Generic, TypeVar, Iterable
import asyncio
import logging
import aiohttp
from aioreactive.core import AsyncSink, AsyncSource, chain
from aioreactive.core import AsyncSingleStream
import json
log = logging.getLogger(__name__)


class MesosSubscripion(AsyncSource):

    def __init__(self, source: AsyncSource) -> None:
        self.source = source

    async def __astart__(self, sink: AsyncSink):
        _sink = await chain(MesosSubscripion.Sink(self), sink)
        return await chain(self.source, _sink)

    class Sink(AsyncSingleStream):

        def __init__(self, source: "DistinctUntilChanged") -> None:
            super().__init__()

        async def asend(self, value: tuple) -> None:
            url, subscription = value
            async def async_worker() -> None:
                try:
                    async with aiohttp.ClientSession(headers={'content-type': 'application/json'}) as session:
                        async with session.post(url, data=json.dumps(subscription),
                                                timeout=100000) as response:
                            async for value in response.content:
                                await self._sink.asend(value)

                except Exception as ex:
                    await self._sink.athrow(ex)
                    return
                await self._sink.aclose()
            try:
                task = asyncio.ensure_future(async_worker())
            except Exception as ex:
                log.debug("MesosSubscripion:worker(), Exception: %s" % ex)
                await self._sink.athrow(ex)


def mesos_subscription(source: AsyncSource) -> AsyncSource:
    """Filters the source stream to have continously distict values.
    """
    return MesosSubscripion(source)
