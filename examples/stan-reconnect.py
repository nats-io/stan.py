# Copyright 2020 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

class Component():

    def __init__(self, loop=None):
        self.sc = None
        self.nc = None
        self._start_seq = 0

        async def conn_lost_cb(err):
            print("Connection lost:", err)
            for i in range(0, 100):
                try:
                    print("Reconnecting...")
                    await self.connect()
                except Exception as e:
                    print("Error", e, type(e))
                    continue
                break
        
        self._stan_conn_lost_cb = conn_lost_cb
        self._loop = loop

    async def connect(self):
        print("Connecting...")
        self.nc = NATS()        
        await self.nc.connect(loop=self._loop)

        self.sc = STAN()
        await self.sc.connect("test-cluster", "client-123",
                              nats=self.nc,
                              ping_interval=1,
                              ping_max_out=5,
                              conn_lost_cb=self._stan_conn_lost_cb,
                              loop=self._loop,
                              )
        async def cb(msg):
            if msg.seq == self._start_seq:
                print("Redelivery after reconnect, skipping... (seq={})".format(msg.seq))
                return

            print("Received a message (seq={}): {}".format(msg.seq, msg.data))
            self._start_seq = msg.seq

        await self.sc.subscribe("hi",
                                start_at='sequence',
                                cb=cb,
                                sequence=self._start_seq,
                                )

async def run(loop):
    c = Component(loop=loop)
    await c.connect()

    for i in range(0, 10000):
        try:
            await c.sc.publish("hi", ('hello world: %d' % i).encode())
        except Exception as e:
            print("Error during publishing:", e)
        await asyncio.sleep(1, loop=loop)

    await self.sc.close()
    await self.nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.run_forever()
    loop.close()
