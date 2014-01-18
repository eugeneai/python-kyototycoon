#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# kt_performance.py - measure performance of the python-kyototycoon library.
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


import sys
import os, os.path

from time import time
from getopt import getopt, GetoptError

from kyototycoon import KyotoTycoon


NUM_ITERATIONS = 2000


def print_usage():
    """Output the proper usage syntax for this program."""

    print("USAGE: %s [-s <host:port>]" % os.path.basename(sys.argv[0]))


def parse_args():
    """Parse and enforce command-line arguments."""

    try:
        options, _ = getopt(sys.argv[1:], "s:", ["server="])
    except GetoptError as e:
        print("error: %s." % e, file=sys.stderr)
        print_usage()
        sys.exit(1)

    server = { "host": "127.0.0.1", "port": 1978 }

    for option, value in options:
        if option in ("-s", "--server"):
            fields = value.strip().split(":")
            server["host"] = fields[0].strip()

            if len(fields) > 1:
                server["port"] = int(fields[1])

    return (server,)


def main():
    server, = parse_args()

    print("Running %d iterations for each parameter..." % NUM_ITERATIONS)

    header = "%-15s | %-16s | %-16s | %-7s | %-14s | %-6s" % \
             ("Binary Protocol", "Unicode Literals", "Packer Type", "Elapsed",
              "Iteration Rate", "Passed")
    print(header)
    print("=" * len(header))

    # Test for both Python 2.x and 3.x...
    unicode_literal = str("") if sys.version_info[0] == 3 else unicode("")
    is_unicode = isinstance("", type(unicode_literal))

    for binary in (True, False):
        for packer_type, packer_name in ((1, "KT_PACKER_PICKLE"),
                                         (2, "KT_PACKER_JSON"),
                                         (3, "KT_PACKER_STRING")):
            kt = KyotoTycoon(binary=binary, pack_type=packer_type)
            kt.open(server["host"], server["port"], timeout=2)

            start = time()
            bad = 0

            for i in range(NUM_ITERATIONS):
                if is_unicode:
                    key = "key-%d-u-%d-%d-café" % (binary, packer_type, i)
                    value = "value-%d-u-%d-%d-café" % (binary, packer_type, i)
                else:
                    key = "key-%d-s-%d-%d-cafe" % (binary, packer_type, i)
                    value = "value-%d-s-%d-%d-cafe" % (binary, packer_type, i)

                kt.set(key, value)
                output = kt.get(key)

                if output != value:
                    bad += 1

                kt.remove(key)
                output = kt.get(key)

                if output is not None:
                    bad += 1

            duration = time() - start
            rate = NUM_ITERATIONS / duration

            print("%-15s | %-16s | %-16s | %5.2f s | %10.2f ips | %-6s" %
                  (binary, is_unicode, packer_name, duration, rate,
                   "No" if bad > 0 else "Yes"))

            kt.close()


if __name__ == "__main__":
    main()


# vim: set expandtab ts=4 sw=4
