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

MB_REPL = 0xb1
MB_SYNC = 0xb0

OP_SET = 0xa1
OP_REMOVE = 0xa2
OP_CLEAR = 0xa5

class KyotoSlave(object):
    def __init__(self, sid, host='127.0.0.1', port=1978, timeout=30):
        if not (0 <= sid <= 65535):
            raise ValueError('SID must fit in a 16-bit unsigned integer')

        self.sid = sid
        self.host = host
        self.port = port
        self.timeout = timeout

    def consume(self, timestamp=None):
        self.socket = socket.create_connection((self.host, self.port), self.timeout)

        start_ts = int(time.time() * 10**9) if timestamp is None else timestamp

        # Ask the server for all available transaction log entries since "start_ts"...
        request = [struct.pack('!BIQH', MB_REPL, 0x00, start_ts, self.sid)]
        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_REPL:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        while True:
            magic, ts = struct.unpack('!BQ', self._read(9))
            if magic == MB_SYNC:  # ...the head of the transaction log has been reached.
                self._write(chr(MB_REPL))
                continue

            if magic != MB_REPL:
                raise KyotoTycoonException('bad response [%s]' % hex(magic))

            # Common log entry information...
            size, = struct.unpack('!I', self._read(4))
            sid, db, db_op = struct.unpack('!HHB', self._read(5))

            if db_op == OP_CLEAR:
                yield {"sid": sid, "db": db, "operation": "clear"}
                continue

            buf = self._read(size - 5)

            if db_op == OP_REMOVE:
                key_size, buf = self._read_varnum(buf)
                key = buf[:key_size]

                yield {"sid": sid, "db": db, "operation": "remove", "key": key}
                continue

            if db_op == OP_SET:
                key_size, buf = self._read_varnum(buf)
                val_size, buf = self._read_varnum(buf)

                key = buf[:key_size]
                val = buf[key_size:]

                # The expiration time is contained in the value portion...
                xt, = struct.unpack('!Q', '\x00\x00\x00' + val[:5])
                val = val[5:]

                yield {"sid": sid, "db": db, "operation": "set", "key": key, "value": val, "expire_ts": xt}
                continue

            raise KyotoTycoonException('unsupported database operation [%s]' % hex(db_op))

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        return True

    def _read_varnum(self, data):
        rp = 0
        value = 0

        while True:
            if rp >= len(data):
                return (0, data)

            curr_byte = ord(data[rp])
            value = (value << 7) + (curr_byte & 0x7f)
            rp += 1

            if curr_byte < 0x80:
                break

        return (value, data[rp:])

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
