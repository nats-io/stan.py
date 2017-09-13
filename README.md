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
    await nc.connect(loop=loop)

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
