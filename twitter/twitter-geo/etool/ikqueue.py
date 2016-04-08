"""
kombu.simple
============

Simple interface.

"""
from __future__ import absolute_import

import socket
import random

from kombu import Connection, Queue, Exchange
from collections import deque
from kombu import messaging
from kombu.connection import maybe_channel
from kombu.five import Empty, monotonic


def connect():
    try:
        result = socket.getaddrinfo("ingest-gnip.pythia.internal", None)
    except socket.gaierror:
        raise

    result = random.choice(result)
    host = result[4][0]

    return Connection("amqp://{}".format(host))


class KReadQueue(object):
    _consuming = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __init__(self, channel, queue, **kwargs):
        self.channel = maybe_channel(channel)
        self.no_ack = kwargs.get("no_ack", False)
        self._queue_declare = kwargs.get("queue_declare", False)
        self.consumer = messaging.Consumer(channel, queue,
                                           auto_declare=False)
        self.inqueues = self.consumer.queues
        self.buffer = deque()
        self.consumer.register_callback(self._receive)
        if self._queue_declare:
            for q in self.inqueues:
                try:
                    q.exchange.declare()
                except Exception as e:  # todo fix this exception
                    print e
                    pass
                q.queue_declare()
                q.queue_bind()
        self._last_message = None
        self._lastq = 0

    def get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()
        self._consume()
        elapsed = 0.0
        remaining = timeout
        while True:
            time_start = monotonic()
            if self.buffer:
                return self.buffer.popleft()
            try:
                self.channel.connection.client.drain_events(
                    timeout=timeout and remaining)
            except socket.timeout:
                raise Empty()
            elapsed += monotonic() - time_start
            remaining = timeout and timeout - elapsed or None

    def get_nowait(self):
        n = len(self.inqueues)
        for i in xrange(n):
            i = (i + self.lastq) % n
            m = self.inqueues[i].get(no_ack=self.no_ack)
            if m is not None:
                self.lastq = i + 1
                return m
        raise Empty()

    def clear(self):
        return self.consumer.purge()

    def qsize(self):
        total = 0
        for q in self.inqueues:
            _, size, _ = q.queue_declare(passive=True)
            total += size
        return size

    def close(self):
        self.consumer.cancel()

    def _receive(self, message_data, message):
        self.buffer.append(message)

    def _consume(self):
        if not self._consuming:
            self.consumer.consume(no_ack=self.no_ack)
            self._consuming = True

    def __len__(self):
        """`len(self) -> self.qsize()`"""
        return self.qsize()


class KWriteQueue(object):
    def __init__(self, channel, exchange, **kwargs):
        self._exchange_declare = kwargs.get("exchange_declare", False)
        if isinstance(exchange, Queue):
            self.exchange = exchange.exchange
        elif isinstance(exchange, basestring):
            self.exchange = Exchange(exchange, type="fanout")  # , durable=True)
        else:
            assert isinstance(exchange, Exchange)
            self.exchange = exchange
        self.channel = maybe_channel(channel)
        self.exchange.maybe_bind(self.channel)
        if self._exchange_declare:
            self.exchange.declare()

        self.producer = messaging.Producer(channel, self.exchange,
                                           serializer='json',
                                           routing_key='',
                                           compression=None,
                                           auto_declare=False)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def put(self, message, serializer=None, headers=None, compression=None,
            routing_key=None, **kwargs):
        self.producer.publish(message,
                              content_type="application/octet-stream",
                              serializer=serializer,
                              routing_key=routing_key,
                              headers=headers,
                              compression=compression,
                              **kwargs)


#class KQueue(KReadQueue, KWriteQueue):
#    def __init__(self, channel, name, mode, no_ack=None, queue_opts=None,
#                 exchange_opts=None, serializer=None,
#                 compression=None, **kwargs):
#        queue = name
#        queue_opts = dict(self.queue_opts, **queue_opts or {})
#        exchange_opts = dict(self.exchange_opts, **exchange_opts or {})
#        if no_ack is None:
#            no_ack = self.no_ack
#        name = queue.name
#        exchange = queue.exchange
#        routing_key = queue.routing_key
#        if mode == "w":
#        else:
#        #producer = None
#        super(SimpleQueue, self).__init__(channel, producer,
#                                          consumer, no_ack, **kwargs)
#        #producer.maybe_declare(self.queue)
#        #consumer.declare()
#        if mode == "w":
#
#        else:
#
