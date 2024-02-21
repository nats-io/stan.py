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

    await sc.connect("test-cluster", "client-123", nats=nc)

    async def cb(msg):
        print(f"Received a message (seq={msg.seq}): {msg.data}")

    sub = await sc.subscribe("hi", deliver_all_available=True, cb=cb)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
    # loop.close()
