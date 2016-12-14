"""Example to show how to split a stream into two substreams."""
import asyncio

from aioreactive.core import start, FuncSink

from aioreactive.producer import Producer, AsyncStream
import aioreactive.producer.operators as ops
from malefico.core.operators.publish import publish


async def main():

    ys = Producer.from_iterable(range(10)) | publish
    odds = ys
    evens = ys

    async def mysink(x):
        print(x)

    await start(odds, FuncSink(mysink))
    await start(evens, FuncSink(mysink))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
