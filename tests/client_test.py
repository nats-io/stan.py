# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from stan.aio.errors import *

import sys
import unittest
from tests.utils import async_test, generate_client_id, start_nats_streaming, \
     StanTestCase, SingleServerTestCase

from time import time

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
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        # Publish a some messages
        packs = []

        for i in range(0, 1024):
            pack = await sc.publish("hi", b'hello')
            packs.append(pack)
            self.assertTrue(len(pack.guid) > 0)
            self.assertEqual(pack.error, "")

        self.assertEqual(len(packs), 1024)

        # Check that we have cleaned up the pub ack map
        self.assertEqual(len(sc._pub_ack_map), 0)
        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_async_publish_and_acks(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        future = asyncio.Future(loop=self.loop)
        packs = []

        # It will be receiving the ack which we will be controlling manually.
        async def cb(ack):
            nonlocal packs
            nonlocal future
            packs.append(ack)
            if len(packs) == 1024:
                future.set_result(True)

        for i in range(0, 1024):
            await sc.publish("hi", b'hello', ack_handler=cb)

        try:
            await asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        # Expect to have received all messages already by now.
        self.assertEqual(len(packs), 1024)

        # Check that we have cleaned up the pub ack map
        self.assertEqual(len(sc._pub_ack_map), 0)

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_async_publish_and_max_acks_inflight(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(),
                         nats=nc, max_pub_acks_inflight=5)

        future = asyncio.Future(loop=self.loop)
        packs = []

        # It will be receiving the ack which we will be controlling manually,.
        async def cb(ack):
            nonlocal packs
            nonlocal future
            packs.append(ack)
            if len(packs) >= 5:
                await asyncio.sleep(0.5, loop=self.loop)

        for i in range(0, 1024):
            future = sc.publish("hi", b'hello', ack_handler=cb)
            try:
                await asyncio.wait_for(future, 0.2, loop=self.loop)
            except Exception as e:
                # Some of them will be timing out since giving up
                # on being able to publish.
                break

        # Gave up with some published acks still awaiting
        self.assertTrue(sc._pending_pub_acks_queue.qsize() > 1)

        # Expect to have received all messages already by now.
        self.assertEqual(len(packs), 5)

        # Waiting some time will let us receive the rest of the messages.
        await asyncio.sleep(2.5, loop=self.loop)
        self.assertEqual(len(packs), 10)

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_subscribe_receives_new_messages(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        # Publish a some messages
        msgs = []
        future = asyncio.Future(loop=self.loop)

        async def cb(msg):
            nonlocal msgs
            msgs.append(msg)
            if len(msgs) == 10:
                future.set_result(True)

        # Start a subscription and wait to receive all the messages
        # which have been sent so far.
        sub = await sc.subscribe("hi", cb=cb)

        for i in range(0, 10):
            await sc.publish("hi", b'hello')

        try:
            asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_unsubscribe_from_new_messages(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        # Publish a some messages
        msgs = []

        async def cb(msg):
            nonlocal msgs
            msgs.append(msg)

        # Start a subscription and wait to receive all the messages
        # which have been sent so far.
        sub = await sc.subscribe("hi", cb=cb)

        for i in range(0, 5):
            await sc.publish("hi", b'hello')

        # Stop receiving new messages
        await sub.unsubscribe()

        for i in range(0, 5):
            await sc.publish("hi", b'hello')

        try:
            asyncio.sleep(2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 5)
        for i in range(0, 5):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_receiving_multiple_subscriptions(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        msgs_a, msgs_b, msgs_c = [], [], []
        async def cb_a(msg):
            nonlocal msgs_a
            # Will not block the dispatching of messages to other subscriptions.
            await asyncio.sleep(0.1, loop=self.loop)
            msgs_a.append(msg)

        async def cb_b(msg):
            nonlocal msgs_b
            msgs_b.append(msg)

        async def cb_c(msg):
            nonlocal msgs_c
            msgs_c.append(msg)

        # Start a subscription and wait to receive all the messages
        # which have been sent so far.
        sub_a = await sc.subscribe("hi", cb=cb_a)
        sub_b = await sc.subscribe("hi", cb=cb_b)
        sub_c = await sc.subscribe("hi", cb=cb_c)

        # All should receive all messages
        for i in range(0, 5):
            await sc.publish("hi", "hello-{}".format(i).encode())

        try:
            await asyncio.sleep(0.25, loop=self.loop)
        except:
            pass

        # Stop receiving new messages
        await sub_a.unsubscribe()
        await sub_b.unsubscribe()
        await sub_c.unsubscribe()

        # No one will receive these messsages
        for i in range(0, 5):
            await sc.publish("hi", b'hello')

        all_msgs = [msgs_b, msgs_c]
        for msgs in all_msgs:
            self.assertEqual(len(msgs), 5)
            for i in range(0, 5):
                m = msgs[i]
                self.assertEqual(m.sequence, i+1)
                self.assertEqual(m.data, 'hello-{}'.format(i).encode())

        # This one was slower at processing the messages,
        # so it should have gotten only a couple.
        self.assertEqual(len(msgs_a), 2)
        for i in range(0, 2):
            m = msgs_a[i]

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_closes_cleans_subscriptions(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        # Publish a some messages
        msgs = []
        future = asyncio.Future(loop=self.loop)

        async def cb(msg):
            nonlocal msgs
            msgs.append(msg)
            if len(msgs) == 10:
                future.set_result(True)

        # Start a subscription and wait to receive all the messages
        # which have been sent so far.
        sub = await sc.subscribe("hi", cb=cb)

        for i in range(0, 10):
            await sc.publish("hi", b'hello')

        try:
            asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        # Need to cleanup STAN session before wrapping up NATS conn.
        await sc.close()

        # Should have removed acks and HBs subscriptions.
        self.assertEqual(len(nc._subs), 0)
        self.assertEqual(sc._hb_inbox, None)
        self.assertEqual(sc._hb_inbox_sid, None)
        self.assertEqual(sc._ack_subject, None)
        self.assertEqual(sc._ack_subject_sid, None)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connecting_with_dup_id(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        client_id = generate_client_id()
        await sc.connect("test-cluster", client_id, nats=nc)

        sc_2 = STAN()
        with self.assertRaises(StanError):
            await sc_2.connect("test-cluster", client_id, nats=nc)

        # Publish a some messages
        msgs = []
        future = asyncio.Future(loop=self.loop)

        async def cb(msg):
            nonlocal msgs
            msgs.append(msg)
            if len(msgs) == 10:
                future.set_result(True)

        # Start a subscription and wait to receive all the messages
        # which have been sent so far.
        sub = await sc.subscribe("hi", cb=cb)

        for i in range(0, 10):
            await sc.publish("hi", b'hello')

        try:
            asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        # Need to cleanup STAN session before wrapping up NATS conn.
        await sc.close()

        # Should have removed acks and HBs subscriptions.
        self.assertEqual(len(nc._subs), 0)
        scs = [sc, sc_2]
        for s in scs:
            self.assertEqual(s._hb_inbox, None)
            self.assertEqual(s._hb_inbox_sid, None)
            self.assertEqual(s._ack_subject, None)
            self.assertEqual(s._ack_subject_sid, None)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connect_timeout_wrong_cluster(self):
        nc = NATS()
        await nc.connect(io_loop=self.loop)

        sc = STAN()
        client_id = generate_client_id()

        with self.assertRaises(ErrConnectReqTimeout):
            await sc.connect("test-cluster-missing", client_id,
                             nats=nc, connect_timeout=0.1)

        await nc.close()
        self.assertFalse(nc.is_connected)

if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
