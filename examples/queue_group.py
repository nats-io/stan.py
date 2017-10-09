# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

async def run(loop):
    nc1 = NATS()
    sc1 = STAN()
    await nc1.connect(io_loop=loop)
    await sc1.connect("test-cluster", "client-1", nats=nc1)

    nc2 = NATS()
    sc2 = STAN()
    await nc2.connect(io_loop=loop)
    await sc2.connect("test-cluster", "client-2", nats=nc2)

    nc3 = NATS()
    sc3 = STAN()
    await nc3.connect(io_loop=loop)
    await sc3.connect("test-cluster", "client-3", nats=nc3)

    group = [sc1, sc2, sc3]

    for sc in group:
        async def queue_cb(msg):
            nonlocal sc
            print("[{}] Received a message on queue subscription: {}".format(msg.sequence, msg.data))

        async def regular_cb(msg):
            nonlocal sc
            print("[{}] Received a message on a regular subscription: {}".format(msg.sequence, msg.data))

        await sc.subscribe("foo", queue="bar", cb=queue_cb)
        await sc.subscribe("foo", cb=regular_cb)

    for i in range(0, 10):
        await sc.publish("foo", 'hello-{}'.format(i).encode())

    await asyncio.sleep(1, loop=loop)

    # Close NATS Streaming session
    for sc in group:
        await sc.close()
    await nc1.close()
    await nc2.close()
    await nc3.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
