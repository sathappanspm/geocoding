#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo explain what this script does
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import os
import binascii
import random
import threading
import time
import json
import uuid
from collections import deque
from .sequences import SequenceRecorder
from .logging_conf import gen_logger
import traceback
from functools import update_wrapper
import atexit
import zmq

logger = gen_logger("solidpubsub")


class MessageParseException(Exception):
    pass


class SolidSocketException(Exception):
    pass

# import zmq.core.socket
# zmq.core.socket._Socket = zmq.core.socket.Socket


class SolidSocket(zmq.Socket):
    def __init__(self, ctx, type, default_timeout=None, throw=True):
        super(SolidSocket, self).__init__(ctx, type)
        self.__dict__['default_timeout'] = default_timeout
        self.__dict__['_throw'] = throw
        self.__dict__["_poller"] = zmq.Poller()
        self.__dict__["_poller"].register(self)

    def __repr__(self):
        return "SS"

    def __str__(self):
        return self.__repr__()

    def on_timeout(self):
        if self__dict__["_throw"]:
            raise SolidSocketException("timed out waiting for reply")
        else:
            return None

    def _timeout_wrapper(f):
        def wrapper(self, *args, **kwargs):
            poller = self.__dict__["_poller"]
            timeout = kwargs.pop('timeout', self.__dict__["default_timeout"])
            if timeout is not None:
                timeout = int(timeout * 1000)
                if not poller:
                    return self.on_timeout()
            else:
                timeout = 1000
                while not poller:
                    pass
            return f(self, *args, **kwargs)
        return update_wrapper(wrapper, f, ('__name__', '__doc__'))

    for _meth in dir(zmq.Socket):
        if _meth.startswith(('send', 'recv')):
            locals()[_meth] = _timeout_wrapper(getattr(zmq.Socket, _meth))

    del _meth, _timeout_wrapper

# zmq.core.Socket = SolidSocket
# import zmq

try:
    from zmq import ssh
except ImportError:
    logger.info("Must install paramiko or pexcept libraries")


class Thread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(Thread, self).__init__(*args, **kwargs)
        self.stop_flag = threading.Event()

    @property
    def go(self):
        return not self.stop_flag.is_set()

    def stop(self):
        self.stop_flag.set()


def zpipe(ctx):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    a.linger = 0
    b = ctx.socket(zmq.PAIR)
    b.linger = 0
    a.hwm = 1
    b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


class Message(object):
    """Docstring for Message """

    __slots__ = ("topic", "sequence", "streamid", "data", "runid")

    def __init__(self, topic, sequence, data, streamid=None, runid=None):
        """@todo: to be defined

        :param topic: @todo
        :param sequence: @todo
        :param data: @todo
        :param runid: @todo

        """
        self.topic = topic
        self.sequence = sequence
        self.streamid = streamid
        self.data = data
        if isinstance(runid, uuid.UUID):
            runid = runid.bytes
        elif runid is None:
            runid = ""
        self.runid = runid

    def send(self, socket, skipjson=False):
        """@todo: Docstring for send

        :param socket: @todo
        :returns: @todo

        """
        if skipjson:
            data = self.data
        else:
            data = json.dumps(self.data, indent=None, separators=(',', ':'))
        sid = self.streamid
        if sid is None:
            sid = ''
        socket.send_multipart([self.topic, sid, str(self.sequence),
                               self.runid, data])

    @classmethod
    def recv(cls, socket):
        msg = socket.recv_multipart()
        if msg is None:
            return None
        try:
            topic, sid, sequence, runid, data = msg
            data = json.loads(data)
            return cls(topic, int(sequence), data, sid, runid)
        except KeyboardInterrupt:
            raise
        except Exception, e:
            logger.error("Failed parsing message\n" + traceback.format_exc())
            raise MessageParseException(e)

    def __repr__(self):
        return u"<Message: stream='%s' run='%s' seq=%d topic='%s' data='%s'>" \
                % (self.streamid, uuid.UUID(bytes=self.runid), self.sequence,
                   self.topic, self.data)


class BasePublisher(Thread):
    """Implements a publisher with status updates"""

    def __init__(self, ctx=None, ttl=10 * 60, ports=(5556, 5557),
                 maxlen=40 * 60 * 60):
        """@todo: to be defined

        :param ctx: zeromq context if you plan on sharing these
        :param ttl: Number of stale data points to keep
        :param ports: Tuple describing ports to connect using
                      (publisher port, syncer port)
        :param syncer: boolean that tells us to start our own syncer

        """
        super(BasePublisher, self).__init__()
        self._ctx = ctx
        self._ttl = ttl
        self._ports = ports
        self._maxlen = maxlen

        if self._ctx is None:
            self._ctx = zmq.Context()

        self.updates, self.pipe = zpipe(self._ctx)
        self._sequence = 0
        self._runid = uuid.uuid1()
        self.publisher = SolidSocket(self._ctx, zmq.PUB)
        self.publisher.bind("tcp://*:%d" % self._ports[0])
        self.syncer = None

    def start_syncer(self):
        self.syncer = PublisherSyncer(self._ctx, self.pipe,
                                      self._ttl, self._ports[1],
                                      maxlen=self._maxlen)
        self.syncer.start()

    def run(self):
        if self.syncer is None:
            self.start_syncer()

        try:
            while self.go:
                results = self.get_data()
                if results is None:
                    if not self.go:
                        break
                    continue
                topic = results[0]
                data = results[1]
                if len(results) == 3:
                    sid = results[2]
                else:
                    sid = None
                self.send_msg(topic, data, sid=sid)
        except KeyboardInterrupt:
            logger.info("Interrupted -- %d messages out" % self._sequence)
        self.cleanup()

    def cleanup(self):
        self._shutdown()
        self.syncer.join()

    def send_msg(self, topic, data, sid=None):
        self._sequence += 1
        if sid is None:
            sid = self.streamid
        msg = Message(topic, self._sequence, data, sid, self._runid)
        msg.send(self.publisher)
        msg.send(self.updates)

    def _shutdown(self):
        msg = Message("command", -1, "exit")
        msg.send(self.publisher)
        msg.send(self.updates)
        self.publisher.close(0)
        self.updates.close(0)

    def get_data(self):
        raise NotImplementedError


class Publisher(Thread):
    """Docstring for Publisher """

    def __init__(self, ctx=None, lport=5558, streamid=None):
        """@todo: to be defined

        :param ctx: zeromq context if you plan on sharing these
        :param lport: port of the receiving end of the DeMuxPublisher
        :param streamid: id or name of stream

        """
        super(Publisher, self).__init__()
        self._ctx = ctx
        if self._ctx is None:
            self._ctx = zmq.Context()
        self._lport = lport
        self._sequence = 0
        self._runid = uuid.uuid1()
        if streamid is None:
            self.streamid = str(uuid.uuid1())
        else:
            self.streamid = streamid

        self.listener = self._ctx.socket(zmq.PUSH)
        self.listener.hwm = 1
        self.listener.connect("tcp://localhost:%d" % self._lport)

    def run(self):
        while self.go:
            results = self.get_data()
            if results is None:
                continue
            topic = results[0]
            data = results[1]
            if len(results) == 3:
                sid = results[2]
            else:
                sid = self.streamid
            self.send_msg(topic, data, sid=sid)

    def send_msg(self, topic, data, sid=None):
        self._sequence += 1
        if sid is None:
            sid = self.streamid
        msg = Message(topic, self._sequence, data, sid, self._runid)
        msg.send(self.listener)

    def get_data(self):
        raise NotImplementedError


class DemuxPublisher(BasePublisher):
    """Docstring for Publisher """

    def __init__(self, ctx=None, lport=5558, **kwargs):
        """@todo: to be defined

        :param lport: @todo
        :param **kwargs: @todo

        """
        super(DemuxPublisher, self).__init__(**kwargs)
        #BasePublisher.__init__(self, **kwargs)
        self._lport = lport

        self.listener = SolidSocket(self._ctx, zmq.PULL, default_timeout=1,
                                    throw=False)
        self.listener.bind("tcp://*:%d" % self._lport)

    def cleanup(self):
        super(DemuxPublisher, self).cleanup()
        self.listener.close(0)

    def get_data(self):
        try:
            msg = Message.recv(self.listener)
        except MessageParseException:
            return None
        if msg is None:
            return None
        return msg.topic, msg.data, msg.streamid


# simple struct for routing information for a key-value snapshot
class Route(object):
    def __init__(self, socket, identity):
        self.socket = socket  # ROUTER socket to send to
        self.identity = identity  # Identity of peer who requested state

    def send(self, msg):
        """Send one state snapshot key-value pair to a socket

        Hash item data is our kvmsg object, ready to send
        """
        # Send identity of recipient first
        self.socket.send(self.identity, zmq.SNDMORE)
        msg.send(self.socket)


class PublisherSyncer(Thread):
    """Implements a publisher with status updates"""

    def __init__(self, ctx, pipe, ttl, port, maxlen=20 * 60 * 60):
        """@todo: to be defined

        :param ctx: @todo
        :param ttl: @todo

        """
        super(PublisherSyncer, self).__init__()

        self._ctx = ctx
        self._pipe = pipe
        self._ttl = ttl
        self._port = port
        self._msgs = deque(maxlen=maxlen)
        if self._ctx is None:
            self._ctx = zmq.Context()

    def run(self):
        """This thread maintains the state and handles requests from clients
        for snapshots.
        """
        self._pipe.send("READY")
        snapshot = self._ctx.socket(zmq.ROUTER)
        snapshot.bind("tcp://*:%d" % self._port)

        poller = zmq.Poller()
        poller.register(self._pipe, zmq.POLLIN)
        poller.register(snapshot, zmq.POLLIN)

        while self.go:
            try:
                items = dict(poller.poll())
            except (zmq.ZMQError, KeyboardInterrupt):
                break  # interrupt/context shutdown

            # Apply state update from main thread
            if self._pipe in items:
                try:
                    msg = Message.recv(self._pipe)
                except MessageParseException:
                    continue
                if msg.topic == "command" and msg.data == "exit":
                    break
                self.update_state(msg)

            # Execute state snapshot request
            if snapshot in items:
                msg = snapshot.recv_multipart()
                try:
                    identity = msg[0]
                    request = msg[1]
                    if request == "sync":
                        runid = msg[2]
                        n = int(msg[3])
                    else:
                        logger.error("Invalid packet type received")
                        continue
                except Exception, e:
                    logger.error("Failed parsing sync packet %s\n%s\n%s\n" %
                                 (str(msg).encode("utf8"), e,
                                  traceback.format_exc()))
                    continue
                # Send state snapshot to client
                route = Route(snapshot, identity)

                # For each entry in kvmap, send kvmsg to client
                for msg in self._msgs:
                    if msg.sequence > n or msg.runid != runid:
                        route.send(msg)

                route.send(Message('command', -1, 'finished'))
        self._pipe.close(0)
        snapshot.close(0)

    def update_state(self, msg):
        self._msgs.append(msg)


class Subscriber(Thread):
    """Implements a publisher with status updates"""

    def __init__(self, ctx=None, ports=(5556, 5557),
                 timeout=None,
                 sequence_start=0,
                 seqfile=None,
                 seqfile_blksize=1024,
                 ssh_args=None):
        """@todo: to be defined

        :param ctx: zeromq context
        :param ports: Tuple describing ports to connect using
                      (publisher port, syncer port)
        :param timeout: timeout in s or None if no timeout.  If timeout
                        reached rebuild the socket and restart
        :param sequence_start: the sequence starting place, ignores runid at
                                startup
        :param seqlfile:  filename to use for sequence storage
        :param seqlfile_blksize:  blocksize of the SequenceRecorder object
        :param ssh_args: if you wish to tunnel over an ssh connection

        """
        super(Subscriber, self).__init__()

        self._ctx = ctx
        self._timeout = timeout
        if seqfile is None:
            self._runid = None
            self._sequence = sequence_start
            self._seqfile = None
        else:
            self._seqfile = SequenceRecorder(seqfile, seqfile_blksize)
            self._runid = self._seqfile.runid
            self._sequence = self._seqfile.seq
            if self._runid is not None:
                print len(self._runid)
                logger.info("Starting at sequence: (%s,%d)" %
                             (uuid.UUID(bytes=self._runid), self._sequence))

        self._ssh_args = ssh_args
        self._ports = ports
        self.tunnels = []
        if self._ctx is None:
            self._ctx = zmq.Context()

    def run(self):
        """run the subscriber, if a timeout occurs automatically rebuild
        connection (including tunnel if required) invoked via start

        :param timeout: timeout in ms

        """
        timeout = self._timeout
        if timeout is None:
            self.run_once()
        else:
            logger.info("Timeout set to: %g" % (timeout))
            while self.go:
                try:
                    self.run_once(timeout)
                except SolidSocketException, e:
                    print "caught", e
                    pass
                for p in self.tunnels:
                    if p.is_alive():
                        p.terminate()
                    ## WARNING: This code is not supported by the
                    ## documented API, keep an eye on it if things change
                    for i, (f, a, k) in enumerate(atexit._exithandlers):
                        if p in a:
                            logger.info("unregistering exithandler for p")
                            del atexit._exithandlers[i]
                            break
                self.tunnels = []
                if self.go:
                    logger.warn("Timed out, restarting at %d" % self._sequence)


    def run_once(self,  timeout=None):
        """run the subscriber, if a timeout occurs return from run loop

        :param timeout: timeout in seconds, None means don't timeout

        """
        # Prepare our context and subscriber
        snapshot = SolidSocket(self._ctx, zmq.DEALER, default_timeout=timeout)
        snapshot.linger = 0
        addr = "tcp://localhost:%d" % self._ports[1]
        if self._ssh_args:
            tunnel = ssh.tunnel_connection(snapshot, addr, **self._ssh_args)
            self.tunnels += [tunnel]
        else:
            snapshot.connect(addr)

        subscriber = SolidSocket(self._ctx, zmq.SUB, default_timeout=timeout)
        subscriber.linger = 0
        subscriber.setsockopt(zmq.SUBSCRIBE, '')
        addr = "tcp://localhost:%d" % self._ports[0]
        if self._ssh_args:
            tunnel = ssh.tunnel_connection(subscriber, addr, **self._ssh_args)
            self.tunnels += [tunnel]
        else:
            subscriber.connect(addr)

        # Get state snapshot
        if self._sequence >= 0:
            logger.info("Requesting shapshot")
            if self._runid is None:
                runid = ""
            else:
                runid = self._runid
            snapshot.send_multipart(["sync", runid, str(self._sequence)])
            while self.go:
                #try:
                msg = Message.recv(snapshot)
                #except:
                #    break          # Interrupted

                if msg.topic == "command":
                    if msg.data == "finished":
                        logger.info("Received snapshot=%d" % self._sequence)
                        break          # Done
                    elif self.data == "exit":
                        break
                else:
                    self._sequence = msg.sequence
                    self._runid = msg.runid
                    self.handle(msg)
                    if self._seqfile is not None:
                        self._seqfile.write(self._sequence, self._runid)

        # Now apply pending updates, discard out-of-sequence messages
        while self.go:
            #try:
            msg = Message.recv(subscriber)
            #except:
            #    self.stop()
            #    break          # Interrupted

            if msg.runid != self._runid and self._runid is not None:
                logger.info("Detecting new runid runid %s != %s" %
                            (uuid.UUID(bytes=self._runid),
                            uuid.UUID(bytes=msg.runid)))
                self._runid = msg.runid
                self._sequence = 0

            if msg.topic == "command" and msg.data == "exit":
                logger.info("Explicit shutdown received lastseq=%d" %
                            self._sequence)
                break          # Done

            #TODO > or >=
            if msg.sequence > self._sequence:
                self._sequence = msg.sequence
                self.handle(msg)
                if self._seqfile is not None:
                    self._seqfile.write(self._sequence, self._runid)

        subscriber.close()
        snapshot.close()

    def handle(self, msg):
        logger.info("recvd: %s" % msg)


if __name__ == '__main__':
    import sys
    b = 0

    class PubbieTester(Publisher):
        def get_data(self):
            i = str(int(random.random() * 100) + b)
            print "send", i
            time.sleep(1.0)
            return ('', i)

    ctx = zmq.Context()
    if len(sys.argv) == 1:
        pub = PubbieTester(ctx)
        mpub = DemuxPublisher(ctx)
        sub = Subscriber(ctx)
        print "Start pubbie"
        print "Start sync"
        pub.start()
        mpub.start()
        print "Start subbie"

        pub.join(3)
        sub.start()
        threads = [pub, sub]
    elif sys.argv[1] == 's':
        sub = Subscriber(ctx, seqfile="test.seq")
        sub.start()
        threads = [sub]
    elif sys.argv[1] == 'p':
        b = 100
        pub = PubbieTester(ctx, streamid="test")
        pub.start()
        threads = [pub]
    elif sys.argv[1] == 'm':
        mpub = DemuxPublisher(ctx)
        mpub.start()
        threads = [mpub]

    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(.1)
    except KeyboardInterrupt:
        for t in threads:
            t.stop()
        for t in threads:
            t.join()
