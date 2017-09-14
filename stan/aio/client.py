# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
import random
import stan.pb.protocol

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

    def __repr__(self):
        return "<nats streaming client v{}>".format(__version__)

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
        """
        Receives heartbeats sent to the client by the server.
        """
        print("HB:", hb)
        pass

    async def _process_ack(self, msg):
        """
        Receives acks from the publishes via the _STAN.acks subscription.
        """
        pub_ack = stan.pb.protocol.PubAck()
        pub_ack.ParseFromString(msg.data)

        # TODO: Unblock pending acks queue
        # TODO: Benchmarking tools
        # TODO: Check for protocol error

        try:
            future = self._pub_ack_map[pub_ack.guid]
            future.set_result(pub_ack)
            del self._pub_ack_map[pub_ack.guid]
        except KeyError:
            # Just skip the pub ack
            return

    async def _process_msg(self, raw_msg):
        """
        Receives the msgs from the STAN subscriptions and replies.
        By default it will reply back with an ack unless manual acking
        was specified in one of the subscription options.
        """
        msg = Msg()

        msg_proto = stan.pb.protocol.MsgProto()
        msg_proto.ParseFromString(raw_msg.data)
        msg.proto = msg_proto

        try:
            sub = self._sub_map[raw_msg.subject]
            msg.sub = sub
        except KeyError:
            # FIXME: async callback to signal error?
            return

        # Yield the message to the subscription callback.
        await sub.cb(msg)

        if not sub.manual_acks:
            # Process auto-ack if not done manually in the callback,
            # by publishing into the ack inbox from the subscription.
            msg_ack = stan.pb.protocol.Ack()
            msg_ack.subject = msg.proto.subject
            msg_ack.sequence = msg.proto.sequence
            await self._nc.publish(sub.ack_inbox, msg_ack.SerializeToString())

    async def ack(self, msg):
        """
        Used to manually acks a message.

        :param msg: Message which is pending to be acked by client.
        """
        ack_proto = stan.pb.protocol.Ack()
        ack_proto.subject = msg.proto.subject
        ack_proto.sequence = msg.proto.sequence
        await self._nc.publish(msg.sub.ack_inbox, ack_proto.SerializeToString())

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
            return future.result()
        except Exception as e:
            # Remove pending future before raising error.
            future.cancel()
            del self._pub_ack_map[guid]
            raise e

    async def subscribe(self, subject,
                        cb=None,
                        start_at=None,
                        deliver_all_available=False,
                        sequence=None,
                        time=None,
                        manual_acks=False,
                        queue=None,
                        ack_wait=DEFAULT_ACK_WAIT,
                        max_inflight=DEFAULT_MAX_INFLIGHT,
                        durable_name=None,
                        ):
        """
        :param subject: Subject for the NATS Streaming subscription.

        :param cb: Callback which will be dispatched the

        :param start_at: One of the following options:
           - 'new_only' (default)
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

        :param durable_name: Name of the durable subscription.
        """
        sub = Subscription(
            subject=subject,
            queue=queue,
            cb=cb,
            manual_acks=manual_acks,
            stan=self,
            )
        self._sub_map[sub.inbox] = sub

        # Should create the NATS Subscription before making the request.
        sid = await self._nc.subscribe(sub.inbox, cb=self._process_msg)
        sub.sid = sid

        req = stan.pb.protocol.SubscriptionRequest()
        req.clientID = self._client_id
        req.maxInFlight = max_inflight
        req.ackWaitInSecs = ack_wait

        if queue is not None:
            req.qGroup = queue

        if durable_name is not None:
            req.durableName = durable_name

        # Normalize start position options.
        if start_at is None or start_at == 'new_only':
            req.startPosition = stan.pb.protocol.NewOnly
        elif start_at == 'last_received':
            req.startPosition = stan.pb.protocol.LastReceived
        elif start_at == 'time':
            req.startPosition = stan.pb.protocol.TimeDeltaStart
            # TODO:
            # req.startTimeDelta...
        elif start_at == 'sequence':
            req.startPosition = stan.pb.protocol.SequenceStart

            # TODO: Check that sequence is defined or error out.
            req.startSequence = sequence
        elif start_at == 'first' or start_at == 'beginning' or deliver_all_available:
            req.startPosition = stan.pb.protocol.First

        # Set STAN subject and NATS inbox where we will be awaiting
        # for the messages to be delivered.
        req.subject = subject
        req.inbox = sub.inbox

        msg = await self._nc.timed_request(
            self._sub_req_subject,
            req.SerializeToString(),
            self._connect_timeout,
            )
        resp = stan.pb.protocol.SubscriptionResponse()
        resp.ParseFromString(msg.data)

        # If there is an error here, then rollback the
        # subscription which we have sent already.
        sub.ack_inbox = resp.ackInbox

        return sub

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

class Subscription(object):

    def __init__(self,
                 subject='',
                 queue='',
                 cb=None,
                 sid=None,
                 durable_name=None,
                 ack_inbox=None,
                 manual_acks=False,
                 stan=None,
                 ):
        self.subject = subject
        self.queue = queue
        self.cb = cb
        self.inbox = new_guid()
        self.sid = sid
        self.ack_inbox = ack_inbox
        self.durable_name = durable_name
        self.manual_acks = manual_acks
        self._sc = stan
        self._nc = stan._nc

    async def unsubscribe(self):
        """
        Remove subscription on a topic in this client.
        """
        await self._nc.unsubscribe(self.sid)

        try:
            del self._sc._sub_map[self.inbox]
        except KeyError:
            pass

        req = stan.pb.protocol.UnsubscribeRequest()
        req.clientID = self._sc._client_id
        req.subject = self.subject
        req.inbox = self.ack_inbox

        if self.durable_name is not None:
            req.durableName = self.durable_name

        msg = await self._nc.timed_request(
            self._sc._unsub_req_subject,
            req.SerializeToString(),
            self._sc._connect_timeout,
            )
        resp = stan.pb.protocol.SubscriptionResponse()
        resp.ParseFromString(msg.data)

        # TODO: handling on unsubscribe errors
        # resp.error...

class Msg(object):

    def __init__(self):
        self.proto = None
        self.sub = None

    @property
    def data(self):
        return self.proto.data

    @property
    def sequence(self):
        return self.proto.sequence

    @property
    def seq(self):
        return self.proto.sequence

    @property
    def timestamp(self):
        return self.proto.timestamp

    def __repr__(self):
        return "<nats streaming msg sequence={}, time={}>".format(self.proto.sequence, self.proto.timestamp)

def new_guid():
    return "%x" % random.SystemRandom().getrandbits(0x58)
