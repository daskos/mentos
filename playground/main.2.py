import asyncio
import json
import aiohttp
from aioreactive.producer import AsyncStream, Producer, op
from aioreactive.core import FuncSink, start
from aioreactive.observable import AsyncAnonymousObserver
from malefico.core.operators.subscription import MesosSubscripion
from malefico.core import recordio as extract

async def decode(x):
    return json.loads(x.decode("ascii"))
async def main():
    url = 'http://localhost:5050/api/v1/scheduler'
    d = {
        "type": "SUBSCRIBE",
        "subscribe": {
            "framework_info": {
                "user":  "username",
                "name":  "Example HTTP Framework"
            }
        }
    }

    headers = {'content-type': 'application/json'}
    xs = Producer(MesosSubscripion(url, d)) | extract | op.map(decode)

    async with start(xs,) as ys:
        async for value in ys:
            print(value)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    asyncio.ensure_future(main(), loop=loop)
    # asyncio.ensure_future(death(), loop=loop)
    loop.run_forever()
    loop.close()
