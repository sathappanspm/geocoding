#!/usr/bin/env python

import sys
import json
import re
import logging
import os
import os.path
import codecs
import time
import conf
import logs

import ikqueue

log = logging.getLogger(__name__)

# constant to select bind() for attaching the socket
BIND = 1
# constant to select connect() for attaching the socket
CONNECT = 2

SERVICE = ""
INITTED = False
KCONNECTION = None

def init(args=None):
    # init logger
    # load/get the config
    # eventually this needs a search path for the config
    # should be env(QFU_CONFIG);./queue.conf;/etc/embers/queue.conf;tcp://localhost:3473
    # use 3473 as the global control channel
    global SERVICE, INITTED
    cf = None
    conf.init(args)
    if args and args.service:
        SERVICE = args.service
    else:
        SERVICE = os.environ.get('UPSTART_JOB', "")

    INITTED = True


def connect(force_new=False):
    global KCONNECTION
    if force_new:
        return ikqueue.connect()
    else:
        if not KCONNECTION:
            KCONNECTION = ikqueue.connect()
        return KCONNECTION


class JsonMarshal(object):
    def __init__(self, encoding='utf8', **kw):
        # raises an error if you get a bogus encoding
        codecs.lookup(encoding)
        self.encoding = encoding
        self.remove_newline = kw.get('remove_newline', False)

    def encode(self, obj):
        msg = json.dumps(obj, encoding=self.encoding, ensure_ascii=False)
        # U+0085(Next Line), U+2028(Line Separator), U+2029(Paragraph Separator)
        if self.remove_newline:
            msg = re.sub(ur'[\u0085\u2028\u2029\n\r\f\v]+', ur'\\n', msg)
            #msg = re.sub(ur'[\u0085\u2028\u2029\n\r\f\v]+|\\n|\\r|\\f|\\v', '\\n', msg)
            #msg = msg.replace("&#133;", '')

        if isinstance(msg, str):
            msg = unicode(msg)

        return msg

    def decode(self, data):
        return json.loads(data, encoding=self.encoding)

    #def send(self, socket, data, flags=0):
    #    socket.send_unicode(data, encoding=self.encoding, flags=flags)

    #def recv(self, socket, flags=0):
    #    b = socket.recv(flags=flags)
    #    return unicode(b, encoding=self.encoding, errors='replace')


class UnicodeMarshal(JsonMarshal):
    def __init__(self, **kw):
        super(UnicodeMarshal, self).__init__(**kw)

    def encode(self, obj):
        return unicode(obj)

    def decode(self, data):
        # exception if this is not decodeable (str, stream etc.)
        return unicode(data)

    # send and recv are handled in JsonMarshall


class RawMarshal(object):
    def encode(self, obj):
        return obj

    def decode(self, obj):
        return obj

    #def send(self, socket, data, flags=0):
    #    if isinstance(data, unicode):
    #        socket.send_unicode(data, flags)
    #    else:
    #        socket.send(data, flags=flags)

    #def recv(self, socket, flags=0):
    #    return socket.recv(flags=flags)


class StreamCaptureProbe(object):
    def __init__(self, encoding='utf8', stream=sys.stdout):
        self._s = codecs.getwriter(encoding)(stream)
        self._s.flush()  # make sure its good

    def __call__(self, action, message):
        if action == Queue.SENT:
            self._s.write(message)
            self._s.write('\n')
            self._s.flush()


class QueueStatsProbe(object):
    def __init__(self, interval_min=5):
        self.interval = datetime.timedelta(minutes=interval_min)
        self.start = datetime.datetime.now()
        self.sent_bytes = 0
        self.sent_msg = 0
        self.recv_bytes = 0
        self.recv_msg = 0

    def __call__(self, action, message):
        if action == Queue.SENT:
            self.sent_bytes += len(message)
            self.sent_msg += 1

        if action == Queue.RECEIVED:
            self.recv_bytes += len(message)
            self.recv_msg += 1

        # TODO - if delta past period report the stats


class Queue(object):
    """Docstring for Queue """
    SENT = 1
    RECEIVED = 2

    def __init__(self, ename, mode, qname="", no_ack=True, capture=False,
                 remove_newline=False, marshal=None, force_new_connection=False):
        """@todo: to be defined

        :param ename: @todo
        :param mode: @todo
        :param qname: @todo
        :param no_ack: @todo
        :param capture: @todo
        :param remove_newline: @todo

        """

        if not INITTED:
            log.warn("QUEUE INIT Not called, calling")
            init()

        self._ename = ename
        self._mode = mode
        self._qname = qname
        self._no_ack = no_ack
        self._probes = []  # probes for tracing events
        self._last_poll = None
        self._marshal = marshal or JsonMarshal()
        self.connection = connect(force_new_connection)

        if not isinstance(self._ename, list):
            self._ename = [self._ename]

        exclusive = (SERVICE == "")
        self._exchanges = [ikqueue.Exchange(e[0], type="fanout", durable=False)
                           for e in self._ename]
        self._queues = [ikqueue.Queue(e[1], ex, exclusive=exclusive,
                                      queue_arguments={"x-max-length": 1000000})
                        for e, ex in zip(self._ename, self._exchanges)]
        self._name = [e[0] for e in self._ename]

    def open(self):
        """@todo: Docstring for open
        :returns: @todo

        """
        if not INITTED:
            init()

        if "r" in self._mode:
            self._queue = ikqueue.KReadQueue(self.connection,
                                            self._queues,
                                            no_ack=self._no_ack,
                                            queue_declare=True)
        elif "w" in self._mode:
            self._queue = ikqueue.KWriteQueue(self.connection,
                                             self._queues[0],
                                             exchange_declare=True)

    def read(self):
        """Reads one message from the queue
        :returns: @todo

        """
        if self._last_poll is not None:
            msg = self._last_poll
            self._last_poll = None
        else:
            msg = self._queue.get(block=True)
        msg = msg.payload
        self.notify(Queue.RECEIVED, msg)
        msg = self._marshal.decode(msg)
        return msg

    def read_without_polling(self):
        """Reads socket without first polling it, guaranteed block if no data
        exists.
        :returns: @todo

        """
        return self.read()

    def poll(self, timeout=None, flags=0):
        if self._last_poll is not None:
            return True
        else:
            try:
                msg = self._queue.get(block=True, timeout=timeout)
            except ikqueue.Empty:
                msg = None
            self._last_poll = msg

        return self._last_poll is not None

    def write(self, data):
        """@todo: Docstring for write

        :param data: @todo
        :returns: @todo

        """
        data = self._marshal.encode(data)
        self._queue.put(data)
        self.notify(Queue.SENT, data)

    def get_name(self):
        if not self._name:
            return None
        elif isinstance(self._name, basestring):
            return self._name
        else:
            return ",".join(self._name)

    # be an iterator
    # http://docs.python.org/library/stdtypes.html#iterator-types
    def __iter__(self):
        return self

    def next(self):
        return self.read()

    # support contextmanager
    # see http://docs.python.org/library/stdtypes.html#context-manager-types
    # with queue.open(...) as q: ...
    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_trace):
        self.close()
        # tell any open control channels we are exiting
        return False

    def close(self):
        """@todo: Docstring for close
        :returns: @todo

        """
        pass

    # probes for tracing messages
    # this is how you can do dumps of messages as they are read/written
    # and stuff like collecting metrics on messages
    def add_probe(self, probe):
        assert hasattr(probe, '__call__'), "Object must be callable."
        self._probes.append(probe)

    def notify(self, action, msg):
        for p in self._probes:
            try:
                p(action, json.dumps(msg))
            except KeyboardInterrupt:
                raise
            except:
                log.exception('Failed to notify probe.')


class StreamQueue(object):
    """
    An object to make a stream (typically stdin or stdout)
    conform to the Queue interface so we can write code that treats
    them interchangeably.
    """

    def __init__(self, stream,
                 mode='r',
                 name=None,
                 encoding='utf8',
                 marshal=JsonMarshal(),
                 end_of_record='\n',
                 **ignore):
        assert stream, "Need to a stream to read or write to."
        assert marshal, "Need a message marshaller to encode and decode messages."

        self._marshal = marshal
        self.end_of_record = end_of_record
        if encoding:
            if mode == 'w':
                self._stream = codecs.getwriter(encoding)(stream, 'replace')
            else:  # default read
                self._stream = codecs.getreader(encoding)(stream, 'replace')
        else:  # accept what they give you
            self._stream = stream

        if not name:
            self._name = None
        else:
            self._name = name

    def get_name(self):
        if not self._name:
            return None
        elif isinstance(self._name, basestring):
            return self._name
        else:
            l = len(self._name)
            if l == 1:
                return self._name[0]
            elif l > 1:
                sout = self._name[0]
                for i in range(1, l):
                    sout = sout + "," + self._name[i]
                return sout
            else:
                return None

    def poll(self, timeout=None, flags=0):  # zmq.POLLIN):
        raise NotImplementedError

    def read(self, flags=0):
        """Read the next item from the stream.
        This deals with blank lines and EOF by passing
        on the values from the stream's read(). Blanks lines
        are a string with a newline (and maybe other whitespace)
        and EOF is returned as ''. I.e. not s.read() => EOF.
        """
        msg = self._stream.readline()
        if msg.strip():  # skip empty lines
            return self._marshal.decode(msg)
        else:  # pass it on - blank line is '\n', EOF is ''
            return msg

    def write(self, obj, flags=0):
        if not obj:
            return
        msg = self._marshal.encode(obj).strip()
        self._stream.write(msg)
        self._stream.write(self.end_of_record)

    def __iter__(self):
        self._iter = self._stream.__iter__()
        return self

    def next(self):
        if self._iter:
            msg = self._iter.next()
            if msg.strip():  # skip blank lines
                return self._marshal.decode(msg)
            else:
                return msg
        else:
            raise Exception('No iterator initialized')

    def close(self):  # No action necessary. Stubbed so this class can follow the usage patterns of other I/O classes
        return

    def __enter__(self):
        self._ctx = self._stream.__enter__()
        return self._ctx

    def __exit__(self, ex_type, ex_val, ex_trace):
        if self._ctx:
            return self._ctx.__exit__()
        else:
            return False


def resolve_address(qname, qtype="r", attach=None):
    """
    Resolve qname into a queue specification,
    either from embers.conf or by treating it as a
    fully qualified name if it is not in the conf.
    Minimal check on form of fully qualified name.
    The attach parameter overrides the default attachment type
    (BIND or CONNECT) for queues doing special connections.
    """
    #(host, port) = conf.get_queue_info(qname)

    if qtype in ("w", ):  # (zmq.PUB, zmq.REP):
        result = (qname, "")
    elif qtype in ("r", ):
        result = (qname, SERVICE)
    else:
        assert False, "Invalid type, Queue no longer supports zmq"

    return result


def get_conf_entry(qname):
    """
    Return the entire JSON expression for a given qname.
    """
    return conf.get_conf_entry(qname)


def open(name, mode='r', capture=False, service=None, exclusive=None, **kw):
    """
    Open a queue with file-like semantics. E.g.:
    q = open('sample-1', 'w') - publish
    q = open('sample-1', 'r') - subscribe
    options:
    name - a queue name, either a full ZMQ-style URL or a name found in queue.conf
    mode - the queue open more. One of r (SUB), w (PUB), r+ (REP), w+ (REQ).
    marshal - class to use to marshal messages, default JsonMarshal
    capture - capture and log messages as they are sent. Can be True, or a stream, or a Capture instance.
    """

    # this is somewhat goofy, but once you have
    # a metaphor you might as well run it into the ground
    assert mode in {"r", "w"}, 'Mode %s is not a valid mode. Use one of r, w'
    typ = mode
    service = service or SERVICE

    # special case '-' -> use stdin or stdout
    if isinstance(name, list) and '-' in name or name == '-':
        if mode in ('w', ):
            s = sys.stdout
            name = 'stdout'
        else:
            s = sys.stdin
            name = 'stdin'

        log.info('Reading from stdin' if name == 'stdin' else 'Writing to stdout')
        return StreamQueue(s, name=name, mode=mode, **kw)

    # normal queue case
    if typ in ("w", ):
        if not name:
            name = conf.get_default_queue_names(service, 'out')

        log.info('Writing to %s' % name)
    else:
        if not name:
            name = conf.get_default_queue_names(service, 'in')

        log.info('Reading from %s' % name)

    if isinstance(name, basestring):
        addr = [resolve_address(name,
                                qtype=typ,
                                attach=kw.get('attach', None))]
    else:
        addr = [resolve_address(n,
                                qtype=typ,
                                attach=kw.get('attach', None))
                for n in name]

    if "qname" in kw:
        qname = kw["qname"]
        addr = [(e[0], qname) for e in addr]

    result = Queue(addr, typ, **kw)

    assert addr, "Could not resolve an address from %s." % (name,)
    result.open()
    if capture:
        result.add_probe(StreamCaptureProbe())

    return result


def main():
    """
    A little utility to handle reading and writing streams
    to and from a queue.
    --pub <queue> : publish what's read from stdin to <queue>
    --sub <queue> : read from <queue> and write the messages to stdout
    --cat         : when used with --pub, write all published messages to stdout
    --clean       : check in incoming and outgoing messages.
                    Verify the message is correct JSON and add
                    an embersId if needed.
    --log_file    : Path to write the log file to
    --log_level   : Logging level
    Other standard EMBERS options (e.g. --verbose).
    """
    import args
    import message
    global log

    ap = args.get_parser()
    ap.add_argument('--clean', action="store_true",
                    help='Verify message format and add standard fields such as embersId.')
    ap.add_argument('--addfeed', action="store_true", help='Add feed and feedPath fields to published message.')
    ap.add_argument('--cat', action="store_true", help='Write all published messages to stdout.')
    ap.add_argument('--rm', nargs="+", help="delete queue")
    arg = ap.parse_args()
    log = logs.getLogger(log_name=arg.log_file)
    logs.init(arg, l=arg.log_level, logfile=arg.log_file)
    init(arg)

    if arg.rm and not arg.sub:
        for queue in arg.rm:
            print "Deleting", queue,
            queue = ikqueue.Queue(queue)
            queue.maybe_bind(connect())
            queue.delete()
            print "."
        return
    try:
        # need to use the raw/utf handler unless we are doing clean
        marshal = UnicodeMarshal()
        if arg.clean or arg.addfeed:
            marshal = JsonMarshal()

        if arg.sub is None and os.environ.get('UPSTART_JOB') is None:
            arg.sub = '-'  # stdin

        subq = open(arg.sub, 'r') #, marshal=marshal, ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)

        if arg.pub is None and os.environ.get('UPSTART_JOB') is None:
            arg.pub = '-'  # stdout

        pubq = open(arg.pub, 'w', capture=arg.cat, marshal=marshal)
    except Exception as e:
        log.exception("Exception opening queues: %s" % e)

    # "Human-readable" queue name can be retrieved as
    #
    # sname = subq.get_name()
    # pname = pubq.get_name()
    rc = 0
    try:
        it = subq.__iter__()
        while True:
            m = ''
            try:
                m = it.next()
                if arg.clean:
                    m = message.clean(m)

                if m:
                    if arg.addfeed:
                        m = message.add_embers_ids(m, feed=pubq.get_name(), feedPath=pubq.get_name())
                    pubq.write(m)
            except StopIteration:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                rc += 1
                if m:
                    log.exception('Could not process message %s: %s' % (m, e))
                else:
                    log.exception('Unknown processing error %s' % e)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        rc = 1
        log.exception('Top level exception %s' % e)

    return rc


if __name__ == '__main__':
    sys.exit(main())
