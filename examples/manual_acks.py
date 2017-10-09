# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

async def run(loop):
    nc = NATS()
    sc = STAN()
    await nc.connect(io_loop=loop)
    await sc.connect("test-cluster", "client-123", nats=nc)

    async def ack_handler(ack):
        print("Received ack: {}".format(ack.guid))
    for i in range(0, 10):
        await sc.publish("foo", b'hello-world', ack_handler=ack_handler)

    async def cb(msg):
        nonlocal sc
        print("Received a message on subscription (seq: {}): {}".format(msg.sequence, msg.data))
        await sc.ack(msg)

    # Use manual acking and have message redelivery be done
    # if we do not ack back in 1 second.
    await sc.subscribe("foo", start_at='first', cb=cb, manual_acks=True, ack_wait=1)

    for i in range(0, 5):
        await asyncio.sleep(1, loop=loop)

    await sc.close()
    await nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
