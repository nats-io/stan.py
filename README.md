# NATS Streaming Python3/Asyncio Client

An asyncio based Python3 client for the [NATS Streaming messaging system](http://nats.io/documentation/streaming/nats-streaming-intro/) platform.

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/stan.py.svg?branch=master)](http://travis-ci.org/nats-io/stan.py)
[![Versions](https://img.shields.io/pypi/pyversions/asyncio-nats-streaming.svg)](https://pypi.org/project/asyncio-nats-streaming)

## Supported platforms

Should be compatible with at least [Python +3.5](https://docs.python.org/3.5/library/asyncio.html).

## Installing

```bash
pip install asyncio-nats-streaming
```

## Basic Usage

```python
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
    await sc.publish("hi", b'hello')
    await sc.publish("hi", b'world')

    total_messages = 0
    future = asyncio.Future(loop=loop)
    async def cb(msg):
        nonlocal future
        nonlocal total_messages
        print("Received a message (seq={}): {}".format(msg.seq, msg.data))
        total_messages += 1
        if total_messages >= 2:
            future.set_result(None)

    # Subscribe to get all messages since beginning.
    sub = await sc.subscribe("hi", start_at='first', cb=cb)
    await asyncio.wait_for(future, 1, loop=loop)

    # Stop receiving messages
    await sub.unsubscribe()

    # Close NATS Streaming session
    await sc.close()

    # We are using a NATS borrowed connection so we need to close manually.
    await nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
```

### Subscription Start (i.e. Replay) Options

NATS Streaming subscriptions are similar to NATS subscriptions,
but clients may start their subscription at an earlier point in the
message stream, allowing them to receive messages that were published
before this client registered interest.

The options are described with examples below:

```python
async def cb(msg):
  print("Received a message (seq={}): {}".format(msg.seq, msg.data))

# Subscribe starting with most recently published value
await sc.subscribe("foo", start_at="last_received", cb=cb)

# Receive all stored values in order
await sc.subscribe("foo", deliver_all_available=True, cb=cb)

# Receive messages starting at a specific sequence number
await sc.subscribe("foo", start_at="sequence", sequence=3, cb=cb)

# Subscribe starting at a specific time by giving a time delta
# with an optional nanoseconds fraction.
# e.g. messages in the last 2 minutes
from time import time
await sc.subscribe("foo", start_at='time', time=time()-120, cb=cb)
```

### Durable subscriptions

Replay of messages offers great flexibility for clients wishing to begin processing
at some earlier point in the data stream. However, some clients just need to pick up where
they left off from an earlier session, without having to manually track their position
in the stream of messages.
Durable subscriptions allow clients to assign a durable name to a subscription when it is created.
Doing this causes the NATS Streaming server to track the last acknowledged message for that
`clientID + durable name`, so that only messages since the last acknowledged message
will be delivered to the client.

```python
# Subscribe with durable name
await sc.subscribe("foo", durable_name="bar", cb=cb)

# Client receives message sequence 1-40
for i in range(1, 40):
  await sc2.publish("foo", "hello-{}".format(i).encode())

# Disconnect from the server and reconnect...

# Messages sequence 41-80 are published...
for i in range(41, 80):
  await sc2.publish("foo", "hello-{}".format(i).encode())

# Client reconnects with same clientID "client-123"
await sc.connect("test-cluster", "client-123", nats=nc)

# Subscribe with same durable name picks up from seq 40
await sc.subscribe("foo", durable_name="bar", cb=cb)
```

### Queue groups

All subscriptions with the same queue name (regardless of the
connection they originate from) will form a queue group. Each message
will be delivered to only one subscriber per queue group, using
queuing semantics. You can have as many queue groups as you wish.

Normal subscribers will continue to work as expected.

#### Creating a Queue Group

A queue group is automatically created when the first queue subscriber
is created. If the group already exists, the member is added to the
group.

```python
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
            print("[{}] Received a message on queue subscription: {}".format(msg.sequence, msg.data))

        async def regular_cb(msg):
            print("[{}] Received a message on a regular subscription: {}".format(msg.sequence, msg.data))

        # Subscribe to queue group named 'bar'
        await sc.subscribe("foo", queue="bar", cb=queue_cb)

        # Notice that you can have a regular subscriber on that subject too
        await sc.subscribe("foo", cb=regular_cb)

    # Clients receives message sequence 1-40 on regular subscription and
    # messages become balanced too on the queue group subscription
    for i in range(0, 40):
        await sc.publish("foo", 'hello-{}'.format(i).encode())

    # When the last member leaves the group, that queue group is removed
    for sc in group:
        await sc.close()
    await nc1.close()
    await nc2.close()
    await nc3.close()
```

### Durable Queue Group

A durable queue group allows you to have all members leave but still
maintain state. When a member re-joins, it starts at the last position
in that group.

#### Creating a Durable Queue Group

A durable queue group is created in a similar manner as that of a
standard queue group, except the `durable_name` option must be used to
specify durability.

```python
async def cb(msg):
   print("[{}] Received a message on durable queue subscription: {}".format(msg.sequence, msg.data))
   
# Subscribe to queue group named 'bar'
await sc.subscribe("foo", queue="bar", durable_name="durable", cb=cb)
```

A group called `dur:bar` (the concatenation of durable name and group
name) is created in the server.

This means two things:

- The character `:` is not allowed for a queue subscriber's durable name.

- Durable and non-durable queue groups with the same name can coexist.

## Advanced Usage

### Asynchronous Publishing

Advanced users may wish to process these publish acknowledgements
manually to achieve higher publish throughput by not waiting on
individual acknowledgements during the publish operation, this can
be enabled by passing a block to `publish`:

```python
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

    # Publish asynchronously by using an ack_handler which
    # will be passed the status of the publish.
    for i in range(0, 1024):
        await sc.publish("foo", b'hello-world', ack_handler=ack_handler)

    async def cb(msg):
        print("Received a message on subscription (seq: {}): {}".format(msg.sequence, msg.data))

    await sc.subscribe("foo", start_at='first', cb=cb)
    await asyncio.sleep(1, loop=loop)

    await sc.close()
    await nc.close()
```

### Message Acknowledgements and Redelivery

NATS Streaming offers At-Least-Once delivery semantics, meaning that
once a message has been delivered to an eligible subscriber, if an
acknowledgement is not received within the configured timeout
interval, NATS Streaming will attempt redelivery of the message.

This timeout interval is specified by the subscription option `ack_wait`,
which defaults to 30 seconds.

By default, messages are automatically acknowledged by the NATS
Streaming client library after the subscriber's message handler is
invoked. However, there may be cases in which the subscribing client
wishes to accelerate or defer acknowledgement of the message. To do
this, the client must set manual acknowledgement mode on the
subscription, and invoke `ack` on the received message:

```python
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
```

## Rate limiting/matching

A classic problem of publish-subscribe messaging is matching the rate of message
producers with the rate of message consumers. Message producers can often outpace
the speed of the subscribers that are consuming their messages. This mismatch is
commonly called a "fast producer/slow consumer" problem, and may result in dramatic
resource utilization spikes in the underlying messaging system as it tries to
buffer messages until the slow consumer(s) can catch up.

### Publisher rate limiting

NATS Streaming provides a connection option called `max_pub_acks_inflight`
that effectively limits the number of unacknowledged messages that a
publisher may have in-flight at any given time.  When this maximum is
reached, further `publish` calls will block until the number of
unacknowledged messages falls below the specified limit.

```python
import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

import time

async def run(loop):
    nc = NATS()
    sc = STAN()
    await nc.connect(io_loop=loop)
    await sc.connect("test-cluster", "client-123", max_pub_acks_inflight=512, nats=nc)

    acks = []
    msgs = []
    async def cb(msg):
        nonlocal sc
        print("Received a message on subscription (seq: {} | recv: {}): {}".format(msg.sequence, len(msgs), msg.data))
        msgs.append(msg)
        await sc.ack(msg)

    # Use manual acking and have message redelivery be done
    # if we do not ack back in 1 second.
    await sc.subscribe("foo", start_at='first', cb=cb, manual_acks=True, ack_wait=1)

    async def ack_handler(ack):
        nonlocal acks
        acks.append(ack)
        print("Received ack: {} | recv: {}".format(ack.guid, len(acks)))

    for i in range(0, 2048):
        before = time.time()
        await sc.publish("foo", b'hello-world', ack_handler=ack_handler)
        after = time.time()
        lag = after-before

        # Async publishing will have backpressured applied if too many
        # published commands are inflight without an ack still.
        if lag > 0.001:
            print("lag at {} : {}".format(lag, i))

    for i in range(0, 5):
        await asyncio.sleep(1, loop=loop)

    await sc.close()
    await nc.close()
```

### Subscriber rate limiting

Rate limiting may also be accomplished on the subscriber side, on a
per-subscription basis, using a subscription option called `max_inflight`.
This option specifies the maximum number of outstanding
acknowledgements (messages that have been delivered but not
acknowledged) that NATS Streaming will allow for a given
subscription.  When this limit is reached, NATS Streaming will suspend
delivery of messages to this subscription until the number of
unacknowledged messages falls below the specified limit.

```python
import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

async def run(loop):
    nc = NATS()
    sc = STAN()
    await nc.connect(io_loop=loop)
    await sc.connect("test-cluster", "client-123", max_pub_acks_inflight=512, nats=nc)

    acks = []
    msgs = []
    async def cb(msg):
        nonlocal sc
        print("Received a message on subscription (seq: {} | recv: {}): {}".format(msg.sequence, len(msgs), msg.data))
        msgs.append(msg)

        # This will eventually add up causing redelivery to occur.
        await asyncio.sleep(0.01, loop=loop)
        await sc.ack(msg)

    # Use manual acking and have message redelivery be done
    # if we do not ack back in 1 second capping to 128 inflight messages.
    await sc.subscribe(
        "foo", start_at='first', cb=cb, max_inflight=128, manual_acks=True, ack_wait=1)

    for i in range(0, 2048):
        await sc.publish("foo", b'hello-world')

    for i in range(0, 10):
        await asyncio.sleep(1, loop=loop)

    await sc.close()
    await nc.close()
```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
