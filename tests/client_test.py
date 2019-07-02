# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from stan.aio.errors import *


import sys
import nats
import time
import unittest
from tests.utils import async_test, generate_client_id, start_nats_streaming, \
     StanTestCase, SingleServerTestCase

from time import time
import logging

class ClientTest(SingleServerTestCase):

    @async_test
    async def test_connect(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

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

        with self.assertRaises(StanError):
            await sc.close()

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connect_with_non_borrowed_nats(self):
        sc = STAN()
        await sc.connect("test-cluster", "client-123", loop=self.loop)

        self.assertTrue(sc._pub_prefix != None)
        self.assertTrue(sc._sub_req_subject != None)
        self.assertTrue(sc._unsub_req_subject != None)
        self.assertTrue(sc._close_req_subject != None)
        self.assertTrue(sc._sub_close_req_subject != None)

        await sc.close()
        self.assertFalse(sc._nc.is_connected)

    @async_test
    async def test_sync_publish_and_acks(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

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
        await nc.connect(loop=self.loop)

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
        await nc.connect(loop=self.loop)

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
        await nc.connect(loop=self.loop)

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
            await asyncio.wait_for(future, 2, loop=self.loop)
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
        await nc.connect(loop=self.loop)

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
            await asyncio.sleep(2, loop=self.loop)
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
        await nc.connect(loop=self.loop)

        sc = STAN()
        await sc.connect("test-cluster", generate_client_id(), nats=nc)

        msgs_a, msgs_b, msgs_c = [], [], []
        async def cb_a(msg):
            nonlocal msgs_a
            msgs_a.append(msg)
            # Will not block the dispatching of messages to other subscriptions.
            await asyncio.sleep(5, loop=self.loop)

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
        self.assertEqual(len(msgs_a), 1)
        m = msgs_a[0]
        self.assertEqual(m.sequence, 1)
        self.assertEqual(m.data, b'hello-0')

        await sc.close()
        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_closes_cleans_subscriptions(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

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
            await asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        # Need to cleanup STAN session before wrapping up NATS conn.
        await sc.close()

        # Should have removed acks and HBs subscriptions.
        self.assertEqual(len(nc._subs), 1)
        self.assertEqual(sc._hb_inbox, None)
        self.assertEqual(sc._hb_inbox_sid, None)
        self.assertEqual(sc._ack_subject, None)
        self.assertEqual(sc._ack_subject_sid, None)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connecting_with_dup_id(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

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
            await asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        # Need to cleanup STAN session before wrapping up NATS conn.
        await sc.close()

        # Should have removed acks and HBs subscriptions.
        self.assertEqual(len(nc._subs), 1)
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
        await nc.connect(loop=self.loop)

        sc = STAN()
        client_id = generate_client_id()

        with self.assertRaises(ErrConnectReqTimeout):
            await sc.connect("test-cluster-missing", client_id,
                             nats=nc, connect_timeout=0.3)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_missing_hb_response_replaces_client(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

        class STAN2(STAN):
            def __init__(self):
                STAN.__init__(self)
            async def _process_heartbeats(self, msg):
                pass

        # Need to reopen this class with a method that
        # does not reply back with the heartbeat message.
        sc = STAN2()
        client_id = generate_client_id()
        await sc.connect("test-cluster", client_id, nats=nc)

        # We have reopened the class so that the first instance
        # will not be replying to the hearbeat ping sent by the
        # server once it finds the duplicated client id.
        sc_2 = STAN()
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
        sub = await sc_2.subscribe("hi", cb=cb)

        for i in range(0, 10):
            await sc_2.publish("hi", b'hello')

        try:
            await asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        self.assertEqual(len(msgs), 10)
        for i in range(0, 10):
            m = msgs[i]
            self.assertEqual(m.sequence, i+1)

        # Need to cleanup STAN session before wrapping up NATS conn.
        await sc_2.close()

        # It will fail because original client has gone away
        # and removed this ID from the cluster.
        with self.assertRaises(StanError):
            await sc.close()

        scs = [sc, sc_2]
        for s in scs:
            self.assertEqual(s._hb_inbox, None)
            self.assertEqual(s._hb_inbox_sid, None)
            self.assertEqual(s._ack_subject, None)
            self.assertEqual(s._ack_subject_sid, None)

        # Should have removed acks and HBs subscriptions.
        self.assertEqual(len(nc._subs), 1)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_reconnect_without_graceful_close(self):
        client_id = generate_client_id()

        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

        with (await nats.connect(loop=self.loop)) as nc:
            # Will timeout as NATS Streaming server considers it
            # continue to be connected...
            with self.assertRaises(ErrConnectReqTimeout):
                sc = STAN()
                await sc.connect("test-cluster", client_id, nats=nc, connect_timeout=0.25)

        with (await nats.connect(loop=self.loop)) as nc:
            # Will timeout as NATS Streaming server considers it
            # continue to be connected since too soon...
            with self.assertRaises(StanError):
                sc = STAN()
                await sc.connect("test-cluster", client_id, nats=nc, connect_timeout=1)

        # If we space out the reconnects then the server will stop
        # detecting the previous instances of the client.
        await asyncio.sleep(1, loop=self.loop)
        with (await nats.connect(loop=self.loop)) as nc:
            # Will timeout as NATS Streaming server considers it
            # continue to be connected...
            with self.assertRaises(ErrConnectReqTimeout):
                sc = STAN()
                await sc.connect("test-cluster", client_id, nats=nc, connect_timeout=0.25)

        await asyncio.sleep(1, loop=self.loop)
        with (await nats.connect(loop=self.loop)) as nc:
            # Will not timeout as NATS Streaming server considers it
            # continue to be connected...
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc, connect_timeout=1)

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
                await asyncio.wait_for(future, 2, loop=self.loop)
            except:
                pass

            self.assertEqual(len(msgs), 10)
            await sc.close()

    @async_test
    async def test_ping_responses_trigger_conn_lost_cb(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

        class STAN2(STAN):
            def __init__(self):
                STAN.__init__(self)
            async def _process_heartbeats(self, msg):
                pass

        expected_client_replaced_str = "client has been replaced or is no longer registered"
        received_error_str = ""
        future = asyncio.Future(loop=self.loop)
        async def conn_lost_cb(err):
            nonlocal received_error_str
            received_error_str = str(err)
            future.set_result(True)

        sc = STAN2()
        client_id = generate_client_id()
        await sc.connect("test-cluster", client_id, nats=nc, ping_interval=1, ping_max_out=10, conn_lost_cb=conn_lost_cb)

        sc_2 = STAN()
        await sc_2.connect("test-cluster", client_id, nats=nc)

        try:
            await asyncio.wait_for(future, 4, loop=self.loop)
        except:
            pass

        self.assertEqual(received_error_str, expected_client_replaced_str)

        await sc_2.close()

        with self.assertRaises(StanError):
            await sc.close()

        self.assertTrue(nc.is_connected)

        await nc.close()
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_missing_ping_responses_trigger_conn_lost_cb(self):
        nc = NATS()
        await nc.connect(loop=self.loop)

        class STAN2(STAN):
            def __init__(self):
                STAN.__init__(self)
            async def _process_ping_response(self, msg):
                pass

        expected_ping_max_out_reached_str = "stan: connection lost due to PING failure"
        received_error_str = ""
        future = asyncio.Future(loop=self.loop)
        async def conn_lost_cb(err):
            nonlocal received_error_str
            received_error_str = str(err)
            future.set_result(True)

        sc = STAN2()
        await sc.connect("test-cluster", generate_client_id(), nats=nc, ping_interval=1, ping_max_out=3, conn_lost_cb=conn_lost_cb)

        try:
            await asyncio.wait_for(future, 5, loop=self.loop)
        except:
            pass

        self.assertEqual(received_error_str, expected_ping_max_out_reached_str)

        await nc.close()
        self.assertFalse(nc.is_connected)

class SubscriptionsTest(SingleServerTestCase):

    @async_test
    async def test_subscribe_start_at_last_received(self):

        msgs   = [] # Durable subscriptions
        qmsgs  = [] # Durable queue subscription
        ndmsgs = [] # Non durable queue subscription
        pmsgs  = [] # Plain subscriptions

        # Durable subscription
        async def cb_foo_quux(msg):
            nonlocal msgs
            msgs.append(msg)

        # Durable queue subscription can coexist with regular subscription
        async def cb_foo_bar_quux(msg):
            nonlocal msgs
            qmsgs.append(msg)

        # Queue subscription
        async def cb_foo_bar(msg):
            nonlocal ndmsgs
            ndmsgs.append(msg)

        # Plain subscription
        async def cb_foo(msg):
            nonlocal pmsgs
            pmsgs.append(msg)

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            sub_foo_quux = await sc.subscribe(
                "foo", durable_name="quux", cb=cb_foo_quux)

            sub_foo_bar_quux = await sc.subscribe(
                "foo", queue="bar", durable_name="quux", cb=cb_foo_bar_quux)

            sub_foo_bar = await sc.subscribe(
                "foo", queue="bar", cb=cb_foo_bar)

            sub_foo = await sc.subscribe(
                "foo", cb=cb_foo)

            for i in range(0, 5):
                await sc.publish("foo", "hi-{}".format(i).encode())

            l = [msgs, qmsgs, ndmsgs, pmsgs]
            for m in l:
                self.assertEqual(len(m), 5)

            await sc.close()

        await asyncio.sleep(1, loop=self.loop)
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc, connect_timeout=10)

            for i in range(5, 10):
                await sc.publish("foo", "hi-{}".format(i).encode())

            sub_foo_quux = await sc.subscribe(
                "foo", durable_name="quux", start_at='last_received', cb=cb_foo_quux)

            sub_foo = await sc.subscribe(
                "foo", start_at="first", cb=cb_foo)

            for i in range(11, 15):
                await sc.publish("foo", "hi-{}".format(i).encode())

            # We should not be able to create a second durable
            # on the same subject.
            before = len(nc._subs)
            with self.assertRaises(StanError):
                await sc.subscribe(
                    "foo", durable_name="quux", cb=cb_foo_quux)
            after = len(nc._subs)
            self.assertEqual(before, after)

            await asyncio.sleep(1, loop=self.loop)
            self.assertEqual(len(msgs), 14)
            self.assertEqual(len(qmsgs), 5)
            self.assertEqual(len(ndmsgs), 5)
            self.assertEqual(len(pmsgs), 19) # Initial 5 + (Initial 5 + New 14)

            await sc.close()

    @async_test
    async def test_close_durable_subscriptions(self):

        msgs   = [] # Durable subscriptions
        qmsgs  = [] # Durable queue subscription
        ndmsgs = [] # Non durable queue subscription
        pmsgs  = [] # Plain subscriptions

        # Durable subscription
        async def cb_foo_quux(msg):
            nonlocal msgs
            msgs.append(msg)

        # Durable queue subscription can coexist with regular subscription
        async def cb_foo_bar_quux(msg):
            nonlocal msgs
            qmsgs.append(msg)

        # Queue subscription
        async def cb_foo_bar(msg):
            nonlocal ndmsgs
            ndmsgs.append(msg)

        # Plain subscription
        async def cb_foo(msg):
            nonlocal pmsgs
            pmsgs.append(msg)

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            sub_foo_quux = await sc.subscribe(
                "foo", durable_name="quux", cb=cb_foo_quux)

            sub_foo_bar_quux = await sc.subscribe(
                "foo", queue="bar", durable_name="quux", cb=cb_foo_bar_quux)

            sub_foo_bar = await sc.subscribe(
                "foo", queue="bar", cb=cb_foo_bar)

            sub_foo = await sc.subscribe(
                "foo", cb=cb_foo)

            for i in range(0, 5):
                await sc.publish("foo", "hi-{}".format(i).encode())

            l = [msgs, qmsgs, ndmsgs, pmsgs]
            for m in l:
                self.assertEqual(len(m), 5)

            subs = [sub_foo_quux, sub_foo_bar_quux, sub_foo_bar, sub_foo]
            for s in subs:
                await s.close()

            # Should not be receiving more messages...
            for i in range(6, 10):
                await sc.publish("foo", "hi-{}".format(i).encode())

            await asyncio.sleep(0.5, loop=self.loop)
            for m in l:
                self.assertEqual(len(m), 5)

            # Double close would result in 'stan: invalid subscription'
            with self.assertRaises(StanError):
                await sub_foo_quux.close()

            await sc.close()

    @async_test
    async def test_distribute_queue_messages(self):
        clients = {}

        class Component:
            def __init__(self, nc, sc):
                self.nc = nc
                self.sc = sc
                self.msgs = []

            async def cb(self, msg):
                self.msgs.append(msg)

        # A...E
        for letter in range(ord('A'), ord('F')):
            nc = NATS()
            sc = STAN()

            client_id = "{}-{}".format(generate_client_id(), chr(letter))
            await nc.connect(name=client_id, loop=self.loop)
            await sc.connect("test-cluster", client_id, nats=nc)
            clients[client_id] = Component(nc, sc)

        for (client_id, c) in clients.items():
            # Messages will be distributed among these subscribers.
            await c.sc.subscribe("tasks", queue="group", cb=c.cb)

        # Get the first client to publish some messages
        acks = []
        future = asyncio.Future(loop=self.loop)
        async def ack_handler(ack):
            nonlocal acks
            acks.append(ack)
            # Check if all messages have been published already.
            if len(acks) >= 2048:
                future.set_result(True)

        pc = list(clients.values())[-1]
        for i in range(0, 2048):
            await pc.sc.publish(
                "tasks", "task-{}".format(i).encode(), ack_handler=ack_handler)

        try:
            # Removing the wait here causes the loop to eventually
            # stop receiving messages...
            await asyncio.wait_for(future, 2, loop=self.loop)
        except:
            pass

        total = 0
        for (client_id, c) in clients.items():
            # Not perfect but all should have had around
            # enough messages...
            self.assertTrue(len(c.msgs) > 400)

            total += len(c.msgs)
            await c.sc.close()
            await c.nc.close()
        self.assertEqual(total, 2048)

    @async_test
    async def test_subscribe_start_at_timestamp(self):

        msgs   = [] # Durable subscriptions
        qmsgs  = [] # Durable queue subscription
        ndmsgs = [] # Non durable queue subscription
        pmsgs  = [] # Plain subscriptions

        # Durable subscription
        async def cb_foo_quux(msg):
            nonlocal msgs
            msgs.append(msg)

        # Durable queue subscription can coexist with regular subscription
        async def cb_foo_bar_quux(msg):
            nonlocal msgs
            qmsgs.append(msg)

        # Queue subscription
        async def cb_foo_bar(msg):
            nonlocal ndmsgs
            ndmsgs.append(msg)

        # Plain subscription
        async def cb_foo(msg):
            nonlocal pmsgs
            pmsgs.append(msg)

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 5):
                await sc.publish("foo", "hi-{}".format(i).encode())

            await asyncio.sleep(1, loop=self.loop)
            for i in range(5, 11):
                await sc.publish("foo", "hi-{}".format(i).encode())

            await asyncio.sleep(2, loop=self.loop)
            for i in range(11, 16):
                await sc.publish("foo", "hi-{}".format(i).encode())

            # Should only get the last 5 messages.
            sub_foo_quux = await sc.subscribe(
                "foo", start_at='time', time=time()-1, durable_name="quux", cb=cb_foo_quux)
            sub_foo = await sc.subscribe(
                "foo", start_at='time', time=time()-1, cb=cb_foo)
            sub_foo_bar_quux = await sc.subscribe(
                "foo", start_at='time', time=time()-1, queue="bar", durable_name="quux", cb=cb_foo_bar_quux)
            sub_foo_bar = await sc.subscribe(
                "foo", start_at='time', time=time()-1, queue="bar", cb=cb_foo_bar)

            await asyncio.sleep(1, loop=self.loop)
            self.assertEqual(len(msgs), 5)
            self.assertEqual(len(qmsgs), 5)
            self.assertEqual(len(ndmsgs), 5)
            self.assertEqual(len(pmsgs), 5)

            # Should receive all the messages now
            sub_foo_everything = await sc.subscribe(
                "foo", start_at='time', time=time()-5, cb=cb_foo)
            await asyncio.sleep(1, loop=self.loop)
            self.assertEqual(len(pmsgs), 21)

            # Skip the first 5.
            i = 0
            for msg in pmsgs[5:]:
                self.assertEqual(msg.data, "hi-{}".format(i).encode())
                i += 1

            await sc.close()

    @async_test
    async def test_subscribe_start_at_sequence(self):

        msgs   = [] # Durable subscriptions
        qmsgs  = [] # Durable queue subscription
        ndmsgs = [] # Non durable queue subscription
        pmsgs  = [] # Plain subscriptions

        # Durable subscription
        async def cb_foo_quux(msg):
            nonlocal msgs
            msgs.append(msg)

        # Durable queue subscription can coexist with regular subscription
        async def cb_foo_bar_quux(msg):
            nonlocal msgs
            qmsgs.append(msg)

        # Queue subscription
        async def cb_foo_bar(msg):
            nonlocal ndmsgs
            ndmsgs.append(msg)

        # Plain subscription
        async def cb_foo(msg):
            nonlocal pmsgs
            pmsgs.append(msg)

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 5):
                await sc.publish("foo", "hi-{}".format(i).encode())

            await asyncio.sleep(1, loop=self.loop)
            for i in range(5, 11):
                await sc.publish("foo", "hi-{}".format(i).encode())

            await asyncio.sleep(2, loop=self.loop)
            for i in range(11, 16):
                await sc.publish("foo", "hi-{}".format(i).encode())

            # Should only get the last 5 messages (12..16, upper bound - lower bound + 1)
            sub_foo_quux = await sc.subscribe(
                "foo", start_at='sequence', sequence=12, durable_name="quux", cb=cb_foo_quux)
            sub_foo = await sc.subscribe(
                "foo", start_at='sequence', sequence=12, cb=cb_foo)
            sub_foo_bar_quux = await sc.subscribe(
                "foo", start_at='sequence', sequence=12, queue="bar", durable_name="quux", cb=cb_foo_bar_quux)
            sub_foo_bar = await sc.subscribe(
                "foo", start_at='sequence', sequence=12, queue="bar", cb=cb_foo_bar)

            await asyncio.sleep(1, loop=self.loop)
            self.assertEqual(len(msgs), 5)
            self.assertEqual(len(qmsgs), 5)
            self.assertEqual(len(ndmsgs), 5)
            self.assertEqual(len(pmsgs), 5)

            # Should receive all the messages now
            sub_foo_everything = await sc.subscribe(
                "foo", start_at='sequence', sequence=1, cb=cb_foo)
            await asyncio.sleep(1, loop=self.loop)
            self.assertEqual(len(pmsgs), 21)

            # Skip the first 5.
            i = 0
            for msg in pmsgs[5:]:
                self.assertEqual(msg.data, "hi-{}".format(i).encode())
                i += 1

            await sc.close()

    @async_test
    async def test_subscribe_with_manual_acks_missing(self):

        pmsgs  = [] # Plain subscription messages

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 100):
                await sc.publish("foo", "hi-{}".format(i).encode())

            async def cb_foo(msg):
                nonlocal pmsgs
                nonlocal sc
                pmsgs.append(msg)

            sub_foo = await sc.subscribe(
                "foo", deliver_all_available=True, manual_acks=True, max_inflight=1, cb=cb_foo)

            # Should try to receive all the messages now
            await asyncio.sleep(1, loop=self.loop)

            # Only get one since not acking back
            self.assertEqual(len(pmsgs), 1)

            await sc.close()

    @async_test
    async def test_subscribe_with_manual_acks(self):

        pmsgs  = [] # Plain subscription messages

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 100):
                await sc.publish("foo", "hi-{}".format(i).encode())

            async def cb_foo(msg):
                nonlocal pmsgs
                nonlocal sc
                await sc.ack(msg)
                pmsgs.append(msg)

            sub_foo = await sc.subscribe(
                "foo", deliver_all_available=True, manual_acks=True, max_inflight=1, cb=cb_foo)

            # Should try to receive all the messages now
            await asyncio.sleep(1, loop=self.loop)

            # Only get one since not acking back
            self.assertEqual(len(pmsgs), 100)

            await sc.close()

    @async_test
    async def test_subscribe_get_pending_queue_size(self):

        pmsgs  = [] # Plain subscription messages

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 100):
                await sc.publish("foo", "hi-{}".format(i).encode())

            async def cb_foo(msg):
                nonlocal pmsgs
                nonlocal sc
                await asyncio.sleep(0.2)
                await sc.ack(msg)
                pmsgs.append(msg)

            sub_foo = await sc.subscribe(
                "foo", deliver_all_available=True, manual_acks=True, cb=cb_foo)

            self.assertTrue(sub_foo.pending_queue_size > 0)
            await asyncio.sleep(1, loop=self.loop)
            await sc.close()

    @async_test
    async def test_subscribe_with_manual_acks_missing_redelivery(self):

        pmsgs  = [] # Plain subscription messages

        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            # Stagger sending messages and then only retrieve based on timestamp
            for i in range(0, 100):
                await sc.publish("foo", "hi-{}".format(i).encode())

            async def cb_foo(msg):
                nonlocal pmsgs
                nonlocal sc
                pmsgs.append(msg)

            sub_foo = await sc.subscribe(
                "foo", ack_wait=1, deliver_all_available=True, manual_acks=True, max_inflight=1, cb=cb_foo)

            # Should try to receive all the messages now
            await asyncio.sleep(2.5, loop=self.loop)

            # Only get one since not acking back, will get same message a few times.
            self.assertEqual(len(pmsgs), 3)

            await sc.close()

    @async_test
    async def test_subscribe_error_callback(self):
        client_id = generate_client_id()
        with (await nats.connect(loop=self.loop)) as nc:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            ex = Exception("random error")
            error_cb_calls = []

            async def cb_foo(msg):
                raise ex

            async def cb_foo_error(err):
                nonlocal error_cb_calls
                error_cb_calls.append(err)

            sub_foo = await sc.subscribe(
                "foo", cb=cb_foo, error_cb=cb_foo_error)

            await sc.publish("foo", b"hi")

            # Should try to receive the message now
            await asyncio.sleep(0.5, loop=self.loop)

            # Error callback should have been called once with our exception
            self.assertEqual(error_cb_calls, [ex])

            await sc.close()

    @async_test
    async def test_subscribe_error_callback_fails(self):
        client_id = generate_client_id()
        with (
            await nats.connect(loop=self.loop)
        ) as nc, self.assertLogs(
            'stan.aio.client', level='ERROR'
        ) as logs:
            sc = STAN()
            await sc.connect("test-cluster", client_id, nats=nc)

            ex = Exception("random error")
            error_cb_calls = []

            async def cb_foo(msg):
                raise ex

            async def cb_foo_error(err):
                nonlocal error_cb_calls
                error_cb_calls.append(err)
                raise err

            sub_foo = await sc.subscribe(
                "foo", cb=cb_foo, error_cb=cb_foo_error)

            await sc.publish("foo", b"hi")

            # Should try to receive the message now
            await asyncio.sleep(0.5, loop=self.loop)

            # Error callback should have been called once with our exception
            self.assertEqual(error_cb_calls, [ex])

            await sc.close()

        # Since our error callback fails, there should be a logging entry
        self.assertTrue(logs.output[0].startswith(
            "ERROR:stan.aio.client:Exception in error callback for subscription to 'foo'"
        ))

if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
