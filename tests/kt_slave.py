#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# kt_slave.py - follow transaction logs from a server.
#
# Copyright (c) 2014 Carlos Rodrigues <cefrodrigues@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#


from __future__ import print_function
from __future__ import division

#from __future__ import unicode_literals


import config

import sys
import os, os.path

from time import time, strftime, localtime
from getopt import getopt, GetoptError

from kyototycoon import KyotoSlave, OP_SET, OP_REMOVE, OP_CLEAR


def print_usage():
    """Output the proper usage syntax for this program."""

    print("USAGE: %s --sid=<sid> [-s <host:port>]" % os.path.basename(sys.argv[0]))


def parse_args():
    """Parse and enforce command-line arguments."""

    try:
        options, _ = getopt(sys.argv[1:], "s:", ["sid="])
    except GetoptError as e:
        print("error: %s." % e, file=sys.stderr)
        print_usage()
        sys.exit(1)

    server = { "host": "127.0.0.1", "port": 1978 }
    sid = None

    for option, value in options:
        if option in ("-s"):
            fields = value.strip().split(":")
            server["host"] = fields[0].strip()

            if len(fields) > 1:
                server["port"] = int(fields[1])
        elif option in ("--sid"):
            sid = int(value)

    if sid is None:
        print("error: slave SID missing.", file=sys.stderr)
        print_usage()
        sys.exit(1)

    return (server, sid)


def main():
    server, sid = parse_args()

    op_name = {OP_SET: "SET", OP_REMOVE: "REMOVE", OP_CLEAR: "CLEAR"}

    slave = KyotoSlave(sid=sid, host=server["host"], port=server["port"])

    try:
        for entry in slave.consume(time()):
            print("[%s] [SID=%s/DB=%d/OP=%s] %s" % (strftime("%Y-%m-%d %H:%M:%S", localtime(time())),
                                                    entry["sid"], entry["db"], hex(entry["op"]),
                                                    op_name[entry["op"]]))

            if entry["op"] == OP_CLEAR:
                continue

            print("  -> key: %s" % repr(entry["key"]))

            if entry["op"] == OP_REMOVE:
                continue

            print("  -> value: %s" % repr(entry["value"]))
            if entry["expires"] < (2**40 - 1):
                print("  -> expires: %s [%s]" % (entry["expires"], strftime("%Y-%m-%d %H:%M:%S", localtime(entry["expires"]))))

    except KeyboardInterrupt:
        print("Exiting on Control-C...")
        slave.close()


if __name__ == "__main__":
    main()


# EOF - kt_slave.py
