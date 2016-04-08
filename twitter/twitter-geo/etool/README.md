
The continuation of a bad start at a common library for EMBERS tools, at least the python ones.

# Usage
The common import to get all of this stuff is:
```python
from etool import args, logs, queue, message
```
This is the namespace setup the examples use.

## args
This provides all of the common arguments for EMBERS programs. See the 
source for the full list. It's a small front end for `argparse`.

Here's how to use it and add a custom argument for your program.
``` python
    import etool.args
    ap = args.get_parser()
    ap.add_argument('--my-arg', metavar='MY_ARG', type=str, required=False,
                    help='An extra argument I want to add.')
    arg = ap.parse_args()
```
Running programs with `--help` works as usual for `argparse`.

## logs
This setups the default logging for a program. It is a simple front end for 
`logging`.

Normal usage is to add:
``` python
import etool.logs
log = logs.getLogger('my_program_name')
```
at the begining of your code as a global, then:
```python
    logs.init(arg)
```
in your `main()`, near the beginning. `arg` is the output of `parse_args()` (see previous).
This sets up the common log format and log location. 

The default log location is `~/logs/program_name.log` (it's really the
current working directory, but this is ~ for production scripts).  It
will put the log in the same directory as the program if it can't find
the `logs` directory in the current working directory.

Then in your code you can just do `log.debug(...)`, `log.error(...)` etc. The best idiom for
exception handlers is:
```python
   ...
   except Exception as e:
     log.exception('opps')
```
This prints the stack trace to the log with the message.

Note that for any libraries you should use the `logging` module so it is compatible
with other scripts.

## queue
This is a wrapper around ZMQ to make it easier to work with. There is an optional 
configuration file called `queue.conf` that it will look for so you can refer to queues by 
name instead of by ZMQ URL. It's a good idea to use this. Put `queue.conf` in your home
directory.

To use it you must initialize the library with:
```python
   import etool.queue
   queue.init(arg)
```
Hopefully the init will go away in the future, but it's needed at the moment because of the
way log initialization works.

Publishing to a queue is simply:
```python
  with queue.open('my-queue', 'w', capture=True) as q:
    msg = {'some': 'data', ...}
    q.write(msg)
```
By default it serializes everything as JSON and sends it as UTF-8 encoded strings. Use this 
method unless you have some very special reason not to. The `capture=True` argument
writes all of the messages sent on the queue to `stdout` so you can log what you send 
and capture it in a file.

Reading from a queue is likewise:
```python
  with queue.open('my-queue', 'r') as q:
    for m in q:
      do_something_with(m)
```

### Advanced queuing
There are other options you can pass to the `open()` routine to control the behavior of 
the queue. Do not use these options without understanding the implications.  Those are:

`context={ZMQContext instance}`: 
Use your own ZMQ context instead of the default. 

`marshall=JsonMarshall`:
Use a custom marshaller. JsonMarshall is the default. Other options are UnicodeMarshall and
RawMarshall. 

`hwm=10000`:
Change the high water mark (number of cached messages) for a queue.

`attach=None`:
Use a non-default queue attachment behavior. By default PUB and REP (write) queues use
bind() to attach the queue. SUB and REQ (read) queues use connect(). There are cases
where you want to do the opposite (e.g. to fan-in a set of PUB queues). Pass
BIND or CONNECT to change this behavior.

`ssh_key=None`:
Pass the path to an SSH key to tunnel a connection. Only supported for connect() attachments.

`ssh_conn=None`:
The name of the host to connect to for SSH tunnels. Only supported for connect() attachments.
If `ssh_key` and `ssh_host` are present a tunnel connection is used.

`use_paramiko=True`:
Use the Paramiko library for SSH tunneled connections. Only relevant when used with the 
`ssh_conn` and `ssh_key` options.

### Command line etool.queue
The `queue` module has a main that does some basic conversion of messages to and from other
sources such as pipes and files. The makes it convenient to replay a set of messages captured
via. `capture=True` to a new queue, or to monitor the traffic on a queue and write the 
output to a file or another process. This program follows the convention of the other
formats and writes one UTF-8 encoded JSON message to each line of output or reads one message
per line from the input. 

There are two main modes you can run things in, publishing or subscribing.
When publishing you typically are reading from a file or a pipe:
```sh
$ python -m etool.queue --pub tcp://*:1234 < my_file_with_messages.txt
```
If you use the `--cat` option it will write the output messages to stdout, e.g.:
```sh
$ python -m etool.queue --pub tcp://*:1234 --cat < my_file_with_messages.txt > my_messages.txt
```

When subscribing you typically give the program a queue and direct the output to a file
or pipe, e.g.
```sh
$ python -m etool.queue --sub datasift-twitter | grep 'hi'
```

The `--clean` option provides a way to ensure the message content is valid JSON and conforms to 
the EMBERS message specification by enforcing the required `embersId` and `date` fields. This 
is typically used when you read from a message archive that may have incomplete or corrupt data, 
and want to republish the cleaned messages to a queue. E.g.
```sh
$ python -m etool.queue --pub tcp://*:34567 --cat --clean < my_archive.txt > my_clean_messages.txt
```

Other processes can then read the cleaned messages as if they were being ingested directly.

Note that it is possible to use `--sub` and `--pub` together, if, e.g. you wanted to capture
a stream with suspect data, clean it and republish it to a new queue, e.g.:
```sh
$ python -m etool.queue --sub orig-queue --pub new-queue --clean --cat > clean_messages.txt
```

Note that any time you use a symbolic queue name (`orig-queue` and `new-queue` in the previous example) these
names must be in queue.conf.

Note that this obsoletes the `replay_queue.py`, `capture_queue.py`, `printq.py` and possibly a few other
legacy scripts.

## message 
This contains some simple utilities for dealing with EMBERS messages, specifically something 
that generates and attaches a new unique `embersId` to a message and a routine for 
converting a date to the EMBERS standard format (ISO 6201). 
E.g.:
```python
m = get_some_data()
m = message.add_embers_ids(m)
m = message.normalize_date(data, ["created_at"])
```
where `m['created_at']` contains the original date. The normalized date is assigned to the
`date` field, per the EMBERS messaging standard. Date normalization is typically only needed
for data consumed from external sources. If you are creating messages you should always use
the ISO date, per the messaging standard.


