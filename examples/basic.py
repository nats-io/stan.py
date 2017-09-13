import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

async def run(loop):
    # Use borrowed connection for NATS then mount NATS Streaming
    # client on top.
    nc = NATS()
    await nc.connect(io_loop=loop)

    # Start session with NATS Streaming cluster.
    sc = STAN()
    await sc.connect("test-cluster", "client-456", nats=nc)

    # Publish a couple of messages
    await sc.publish("hi", b'hello')
    await sc.publish("hi", b'world')

    # Subscribe to get all messages since beginning.
    await sc.subscribe("hi", start_at='first')

    # Wrap up NATS Streaming session
    await sc.close()

    # We are using a borrowed connection so we need to close manually.
    await nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
