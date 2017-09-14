# NATS Streaming Asyncio Client

An asyncio-based ([PEP 3156](https://www.python.org/dev/peps/pep-3156/)) Python 3 client for the [NATS Streaming messaging system](http://nats.io/documentation/streaming/nats-streaming-intro/) platform.

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

## Supported platforms

Should be compatible with at least [Python +3.6](https://docs.python.org/3.6/library/asyncio.html).

## Getting Started

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

    async def cb(msg):
        print("Received a message (seq={}): {}".format(msg.seq, msg.data))

    # Subscribe to get all messages since beginning.
    sub = await sc.subscribe("hi", start_at='first', cb=cb)
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
await sc.subscribe("foo", start_at="last_received")

# Receive all stored values in order
await sc.subscribe("foo", deliver_all_available=True)

# Receive messages starting at a specific sequence number
await sc.subscribe("foo", start_at="sequence", sequence=3)

# Subscribe starting at a specific time by giving a unix timestamp
# with an optional nanoseconds fraction
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
await sc.subscribe("foo", durable_name: "bar", cb=cb)

# Client receives message sequence 1-40
for i in range(1, 40):
  await sc2.publish("foo", "hello-{}".format(i))

# Disconnect from the server and reconnect...

# Messages sequence 41-80 are published...
for i in range(41, 80):
  await sc2.publish("foo", "hello-{}".format(i))

# Client reconnects with same clientID "client-123"
await sc.connect("test-cluster", "client-123", nats: nc)

# Subscribe with same durable name picks up from seq 40
await sc.subscribe("foo", durable_name: "bar", cb=cb)
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
```

## License

(The MIT License)

Copyright (c) 2017 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
