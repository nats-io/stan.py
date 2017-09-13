import asyncio

from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from tests.utils import async_test, start_nats_streaming, StanTestCase, SingleServerTestCase

class ClientTest(SingleServerTestCase):

    @async_test
    async def test_connect(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", "client-123", nats=nc)

        self.assertTrue(sc._pub_prefix != None)
        self.assertTrue(sc._sub_req_subject != None)
        self.assertTrue(sc._unsub_req_subject != None)
        self.assertTrue(sc._close_req_subject != None)
        self.assertTrue(sc._sub_close_req_subject != None)

        # Connection borrowed so if we close it is still ok.
        await sc.close()
        self.assertTrue(nc.is_connected)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_sync_publish_and_acks(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", "client-123", nats=nc)

        # Publish a some messages
        packs = []

        for i in range(0, 10):
            pack = await sc.publish("hi", b'hello')
            packs.append(pack)
            self.assertTrue(len(pack.guid) > 0)
            self.assertEqual(pack.error, "")

        self.assertEqual(len(packs), 10)

        # Check that we have cleaned up the pub ack map
        self.assertEqual(len(sc._pub_ack_map), 0)

        await nc.close()
        self.assertFalse(nc.is_connected)
