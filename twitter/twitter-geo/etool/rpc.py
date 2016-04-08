#!/usr/bin/env python

import gevent.monkey
gevent.monkey.patch_all()

import gevent
from gevent.coros import BoundedSemaphore
import gevent.event

import json
import conf
import uuid
from . import queue
from . import kqueue
from . import logs
from kombu import messaging

log = logs.getLogger(__name__)

INITTED = False
KCONNECTION = None


def init(args=None):
    # init logger
    # load/get the config
    # eventually this needs a search path for the config
    # should be env(QFU_CONFIG);./queue.conf;/etc/embers/queue.conf;tcp://localhost:3473
    # use 3473 as the global control channel
    global INITTED
    conf.init(args)

    INITTED = True


def connect(force_new=False):
    global KCONNECTION
    if force_new:
        return kqueue.connect()
    else:
        if not KCONNECTION:
            KCONNECTION = kqueue.connect()
        return KCONNECTION

RPC_PREFIX = "rpc:"


class Client(object):
    """Docstring for rpc.Client"""

    def __init__(self, name, force_new_connection=False):
        """@todo: to be defined

        :param name: @todo
        :param force_new_connection: @todo

        """

        if not INITTED:
            log.warn("QUEUE INIT Not called, calling")
            init()

        self._name = RPC_PREFIX + name
        self._connection = connect(force_new_connection)
        self._channel = self._connection.channel()
        self._queue = self._connection.SimpleQueue("", queue_opts={"exclusive": True})
        self._marshal = queue.JsonMarshal()
        self._producer = messaging.Producer(self._channel, routing_key=self._name)

    def __call__(self, **kwargs):
        self._last_corr_id = str(uuid.uuid4())
        body = json.dumps(kwargs, encoding="ascii")
        properties = {"reply_to": self._queue.queue.name,
                      "correlation_id": self._last_corr_id}
        self._producer.publish(body, **properties)

        message = self._queue.get()
        while message.properties["correlation_id"] != self._last_corr_id:
            message.ack()
            message = self._queue.get()
        message.ack()
        msg = self._marshal.decode(message.payload)
        return msg


class ClientMT(object):
    """Docstring for rpc.Client"""

    def __init__(self, name, force_new_connection=False):
        """@todo: to be defined

        :param name: @todo
        :param force_new_connection: @todo

        """

        if not INITTED:
            log.warn("QUEUE INIT Not called, calling")
            init()

        self._name = RPC_PREFIX + name
        self._connection = connect(force_new_connection)
        self._channel = self._connection.channel()
        self._queue = self._connection.SimpleQueue("", queue_opts={"exclusive": True})
        self._marshal = queue.JsonMarshal()
        self._producer = messaging.Producer(self._channel, routing_key=self._name)
        self._lock = BoundedSemaphore(1)
        self.reqs = {}

    def __call__(self, **kwargs):
        corr_id = str(uuid.uuid4())
        self.reqs[corr_id] = gevent.event.AsyncResult()
        for retry in range(3):  # 3 retries
            body = json.dumps(kwargs, encoding="ascii")
            properties = {"reply_to": self._queue.queue.name,
                          "correlation_id": corr_id}
            # with self._lock:
            self._producer.publish(body, **properties)
            try:
                result = self.reqs[corr_id].get(timeout=10)
                del self.reqs[corr_id]
                return result
            except gevent.event.Timeout:
                log.warn("%s: Timed out retry=%d", self._name, retry)
                pass
        log.warn("%s: Timed out too many times", self._name)
        del self.reqs[corr_id]

    def start_poll(self):
        self._polllet = gevent.spawn(self.poll)

    def stop_poll(self):
        self._polllet.kill()

    def poll(self):
        try:
            while True:
                try:
                    message = self._queue.get_nowait()
                except kqueue.Empty:
                    gevent.sleep(0)  # allow other threads a chance
                    continue

                corr_id = message.properties["correlation_id"]
                if corr_id not in self.reqs:
                    log.warn("%s: ran into corr_id that was not sent", self._name)
                    message.ack()
                    continue
                msg = self._marshal.decode(message.payload)
                self.reqs[corr_id].set(msg)
                message.ack()
        except KeyboardInterrupt:
            log.info("Gracefully quitting from KeyboardInterrupt")
            return
        except gevent.GreenletExit:
            log.info("Gracefully quitting from GreenletExit")
            return
