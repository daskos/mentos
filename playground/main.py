import asyncio
import json
import aiohttp
from aioreactive.core import FuncSink, start
from aioreactive.producer import AsyncStream, Producer, op
from malefico.core import recordio as extract
from functools import partial
headers = {'content-type': 'application/json'}
from retrial.retrial import retry

stream = AsyncStream()

class Test:
    def __init__(self,iter):
        self.iter= iter
    async def __aiter__(self):
  # ^ note
        return self
    async def __anext__(self):
        try:
            async for v in self.iter:
                return v
        except StopIteration:
            raise 
        except Exception as e:
            # or whatever kind of logging you want
            print("Connection to master dropped")
            raise StopAsyncIteration


def decode(msg):
    return json.loads(msg.decode("ascii"))



#@retry()
async def subscribe(session, subscription):

    url, message = subscription
    try:
        response = await session.post(url, data=json.dumps(message),
                                  timeout=100000)
        return Producer.from_iterable(Test(response.content))
    except Exception as ex:
        session.close()
        response.close() 

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
    session = aiohttp.ClientSession(headers=headers)
    subscribe_s = asyncio.coroutine(partial(subscribe, session))

    m_stream = (stream
                | op.flat_map(subscribe_s)
                | extract
                | op.map(decode)
                )

    intermediate = AsyncStream()
    # heartbeat = (m_stream | op.filter(
    #     lambda x: x["type"] == "HEARTBEAT") | op.map(lambda x: 1))

    # subscribed = (m_stream | op.filter(
    #     lambda x: x["type"] == "SUBSCRIBED") | op.map(lambda x: 2))

    # offers = (m_stream | op.filter(
    #     lambda x: x["type"] == "OFFERS") | op.map(lambda x: 3))
    heartbeat = (intermediate)

    subscribed = (intermediate)

    offers = (intermediate)

    async def asend(value):
        print(value)

    ys = await start(heartbeat, FuncSink(asend))
    ys2 = await start(subscribed, FuncSink(asend))
    ys3 = await start(offers, FuncSink(asend))
    await start(m_stream, intermediate)
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
