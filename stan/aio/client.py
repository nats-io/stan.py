# Copyright 2017 Apcera Inc. All rights reserved.

import asyncio
import stan.pb.protocol

__version__ = '0.1.0'

# Subject namespaces for clients to ack and connect
DEFAULT_ACKS_SUBJECT     = "_STAN.acks"
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
    """Asyncio Client for NATS Streaming
    """

    def __init__(self):
        self._nc = None
        self._borrowed = False
        self._cluster = None
        self._connect_timeout = None

    @asyncio.coroutine
    def connect(self, cluster,
                nats=None,
                connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                ):
        """
        Starts a session with a NATS Streaming cluster.

        :param cluster: Name of the cluster to which we will connect.
        :param nats: NATS connection to be borrowed for NATS Streaming.
        """
        self._cluster = cluster
        self._nc = nats
        creq = stan.pb.protocol.ConnectRequest()
        payload = creq.SerializeToString()

        msg = yield from self._nc.timed_request(DEFAULT_DISCOVER_SUBJECT % cluster, payload, self._connect_timeout)
        print(msg.data)
        cresp = stan.pb.protocol.ConnectResponse()
        response = cresp.ParseFromString(msg.data)
        print(response)
        # How to decode a message

    @asyncio.coroutine
    def publish(self, subject, payload, ack_handler=None):
        """
        Publishes a payload onto a subject.  By default, it will block
        until the message which has been published has been acked back.
        An optional async handler can be publi

        :param subject: Subject of the message.
        :param payload: Payload of the message which wil be published.
        :param ack_handler: Optional handler for async publishing.
        """
        pass

    @asyncio.coroutine
    def subscribe(self, subject,
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
