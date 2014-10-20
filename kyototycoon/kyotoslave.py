# -*- coding: utf-8 -*-
#
# Copyright 2014, Carlos Rodrigues
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.

import socket
import struct
import time

from .kt_error import KyotoTycoonException

MB_REPLICATION = 0xb1
MB_SYNCED = 0xb0

class KyotoSlave(object):
    def __init__(self, sid, host='127.0.0.1', port=1978, timeout=30):
        if not (0 <= sid <= 65535):
            raise ValueError('SID must fit in a 16-bit unsigned integer')

        self.sid = sid
        self.host = host
        self.port = port
        self.timeout = timeout

    def consume(self):
        self.socket = socket.create_connection((self.host, self.port), self.timeout)

        kt_now = int(time.time() * 1000) * 10**6
        request = [struct.pack('!BIQH', MB_REPLICATION, 0, kt_now, self.sid)]
        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_REPLICATION:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        while True:
            magic, ts = struct.unpack('!BQ', self._read(9))
            if magic == MB_SYNCED:
                self._write(chr(MB_REPLICATION))
                continue

            size, = struct.unpack('!I', self._read(4))
            log = self._read(size)

            # TODO: improve this
            yield log

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        return True

    def _write(self, data):
        self.socket.sendall(data)

    def _read(self, bytecnt):
        buf = []
        read = 0
        while read < bytecnt:
            recv = self.socket.recv(bytecnt - read)
            if not recv:
                raise IOError('no data while reading')

            buf.append(recv)
            read += len(recv)

        return b''.join(buf)

# EOF - kyotoslave.py
