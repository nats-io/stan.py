# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
import stan.pb.protocol
import random

__version__ = '0.1.0'

# Subject namespaces for clients to ack and connect
DEFAULT_ACKS_SUBJECT = "_STAN.acks.%s"
DEFAULT_DISCOVER_SUBJECT = "_STAN.discover.%s"

# Ack timeout in seconds
DEFAULT_ACK_WAIT = 30

# Max number of inflight acks from received messages
DEFAULT_MAX_INFLIGHT = 1024

# Connect timeout in seconds
DEFAULT_CONNECT_TIMEOUT = 2

# Max number of inflight pub acks
DEFAULT_MAX_PUB_ACKS_INFLIGHT = 16384

class Client:
    """
    Asyncio Client for NATS Streaming.
    """

    def __init__(self):
        # NATS transport.
        self._nc = None
        self._loop = None
        self._nats_conn_is_borrowed = False

        # Options
        self._connect_timeout = None
        self._max_pub_acks_inflight = None

        # Inbox subscription for periodical heartbeat messages.
        self._hb_inbox = None
        self._hb_inbox_sid = None

        # Subscription for processing received acks from the server.
        self._ack_subject = None
        self._ack_subject_sid = None

        # Publish prefix set by stan to which we append our subject on publish.
        self._pub_prefix = None
        self._sub_req_subject = None
        self._unsub_req_subject = None
        self._close_req_subject = None
        self._sub_close_req_subject = None

        # Map of guid to futures which are signaled when the ack is processed.
        self._pub_ack_map = {}

        # Map of subscriptions related to the NATS Streaming session.
        self._sub_map = {}

    async def connect(self, cluster_id, client_id,
                      nats=None,
                      connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                      max_pub_acks_inflight=DEFAULT_MAX_PUB_ACKS_INFLIGHT,
                      loop=None,
                      ):
        """
        Starts a session with a NATS Streaming cluster.

        :param cluster: Name of the cluster to which we will connect.
        :param nats: NATS connection to be borrowed for NATS Streaming.
        """
        self._cluster_id = cluster_id
        self._client_id = client_id
        self._loop = loop

        if nats is not None:
            self._nc = nats
            # NATS Streaming client should use same event loop
            # as the borrowed NATS connection.
            self._loop = self._nc._loop

        # Subjects
        self._discover_subject = DEFAULT_DISCOVER_SUBJECT % self._cluster_id
        self._hb_inbox = new_guid()
        self._ack_subject = DEFAULT_ACKS_SUBJECT % new_guid()

        # Heartbeats subscription
        self._hb_inbox_sid = await self._nc.subscribe(
            self._hb_inbox,
            cb=self._process_heartbeats,
            )

        # Acks processing subscription
        self._ack_subject_sid = await self._nc.subscribe(
            self._ack_subject,
            cb=self._process_ack,
            )
        await self._nc.flush()

        # Start NATS Streaming session by sending ConnectRequest
        creq = stan.pb.protocol.ConnectRequest()
        creq.clientID = self._client_id
        creq.heartbeatInbox = self._hb_inbox
        payload = creq.SerializeToString()

        msg = await self._nc.timed_request(
            self._discover_subject,
            payload,
            self._connect_timeout,
            )

        # We should get the NATS Streaming subject from the
        # response from the ConnectRequest.
        resp = stan.pb.protocol.ConnectResponse()
        resp.ParseFromString(msg.data)
        self._pub_prefix = resp.pubPrefix
        self._sub_req_subject = resp.subRequests
        self._unsub_req_subject = resp.unsubRequests
        self._close_req_subject = resp.closeRequests
        self._sub_close_req_subject = resp.subCloseRequests

    async def _process_heartbeats(self, hb):
        print("HB:", hb)
        pass

    async def _process_ack(self, msg):
        pub_ack = stan.pb.protocol.PubAck()
        pub_ack.ParseFromString(msg.data)

        # TODO: Unblock pending acks queue
        # TODO: Benchmarking tools
        # TODO: Check for protocol error

        try:
            future = self._pub_ack_map[pub_ack.guid]
            future.set_result(pub_ack)
        except KeyError:
            # Just skip the pub ack
            return

    async def publish(self, subject, payload,
                      ack_handler=None,
                      ack_wait=DEFAULT_ACK_WAIT,
                      ):
        """
        Publishes a payload onto a subject.  By default, it will block
        until the message which has been published has been acked back.
        An optional async handler can be publi

        :param subject: Subject of the message.
        :param payload: Payload of the message which wil be published.
        :param ack_handler: Optional handler for async publishing.
        :param ack_wait: How long in seconds to wait for an ack to be received.
        """

        stan_subject = ''.join([self._pub_prefix, '.', subject])
        guid = new_guid()
        pe = stan.pb.protocol.PubMsg()
        pe.clientID = self._client_id
        pe.guid = guid
        pe.subject = subject
        pe.data = payload

        # Process asynchronously if a handler is given.
        if ack_handler is None:
            pass

        # Synchronous wait for ack handling.
        future = asyncio.Future(loop=self._loop)
        self._pub_ack_map[guid] = future

        try:
            await self._nc.publish_request(
                stan_subject,
                self._ack_subject,
                pe.SerializeToString(),
                )
            await asyncio.wait_for(future, ack_wait, loop=self._loop)
        except Exception as e:
            # Remove pending future before raising error.
            future.cancel()
            del self._pub_ack_map[guid]
            raise e

    async def subscribe(self, subject,
                  start_at='',
                  deliver_all_available=False,
                  sequence=None,
                  time=None,
                  manual_acks=False,
                  ack_wait=None,
                  ):
        """
        :param start_at: One of the following options:
           - 'first'
           - 'sequence'
           - 'last_received'
           - 'time'

        :param deliver_all_available: Signals to receive all messages.

        :param sequence: Start sequence position from which we will be
        receiving the messages.

        :param time: Unix timestamp after which the messages will be delivered.

        :param manual_acks: Disables auto ack functionality in the subscription
        callback so that it is implemented by the user instead.

        :param ack_wait: How long to wait for an ack before being redelivered
        previous messages.
        """
        pass

    async def close(self):
        """
        Close terminates a session with NATS Streaming.
        """
        req = stan.pb.protocol.CloseRequest()
        req.clientID = self._client_id
        msg = await self._nc.timed_request(
            self._close_req_subject,
            req.SerializeToString(),
            self._connect_timeout,
            )
        resp = stan.pb.protocol.CloseResponse()
        resp.ParseFromString(msg.data)

        # TODO: check error
        # TODO: remove all the related subscriptions
        # TODO: remove the core NATS Streaming subscriptions
        # TODO: close connection if it was borrowed

def new_guid():
    return "%x" % random.SystemRandom().getrandbits(0x58)
