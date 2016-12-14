import asyncio
import json
import aiohttp
from aioreactive.producer import AsyncStream, Producer, op
from aioreactive.core import FuncSink, start
from aioreactive.observable import AsyncAnonymousObserver
from malefico.core.operators.subscription_subject import mesos_subscription
from malefico.core import recordio as extract
from malefico.core.operators.publish import publish
from functools import partial

async def subscribe(subscription, session):

    url, message = subscription
    request = session.post(url, data=json.dumps(
        message), timeout=100000)
    response = await request
    return Producer.from_iterable(response.content)


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

    session = aiohttp.ClientSession(
        headers={'content-type': 'application/json'})
    stream = AsyncStream()
    m_stream = (stream | op.map(partial(subscribe, session)) |
                extract | op.map(decode))

    intermediate = AsyncStream()
    # heartbeat = (m_stream | op.filter(lambda x: x["type"] == "HEARTBEAT"))

    # subscribed = (m_stream | op.filter(lambda x: x["type"] == "SUBSCRIBED"))

    # offers = (m_stream | op.filter(lambda x: x["type"] == "OFFERS"))
    heartbeat = (m_stream)

    subscribed = (m_stream)

    offers = (m_stream)

    async def asend(value):
        print(value)

    ys = await start(heartbeat, FuncSink(asend))
    ys2 = await start(subscribed, FuncSink(asend))
    ys3 = await start(offers, FuncSink(asend))
    # await start(m_stream, intermediate)
    await stream.asend((url, d))
    # async for value in ys:
    #     print(value)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    asyncio.ensure_future(main(), loop=loop)
    # asyncio.ensure_future(death(), loop=loop)
    loop.run_forever()
    loop.close()
