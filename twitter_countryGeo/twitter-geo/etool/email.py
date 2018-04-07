#!/usr/bin/env python

import boto
from . import logs

log = logs.getLogger(__name__)

conn = None


def init(options):
    global conn
    conn = boto.connect_ses(options.aws_key, options.aws_secret)


def send_email(frm, to, subject, body):
    """TODO: Docstring for send_email.

    :param frm: TODO
    :param to: TODO
    :param subject: TODO
    :param body: TODO
    :returns: TODO

    """
    if not isinstance(to, list):
        to = [to]
    if conn is None:
        log.error("Email library not initialized")

    conn.send_email(frm, subject, body, to)


def main(args):
    from . import args
    ap = args.get_parser()
    ap.add_argument("frm")
    ap.add_argument("to")
    ap.add_argument("subject")
    ap.add_argument("body")
    options = ap.parse_args()

    init(options)
    send_email(options.frm, options.to, options.subject, options.body)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))
