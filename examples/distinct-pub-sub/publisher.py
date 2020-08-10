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

    # Synchronous Publisher, does not return until an ack
    # has been received from NATS Streaming.
    for _ in range(50):
        await sc.publish("hi", b"Hello, World!")

    print("Published 50 messages on subject: hi")
    # Close NATS Streaming session
    await sc.close()

    # We are using a NATS borrowed connection so we need to close manually.
    await nc.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
