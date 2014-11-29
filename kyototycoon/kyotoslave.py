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
        '''Initialize a Kyoto Tycoon replication slave with ID "sid" to the specified master.'''

        if not (0 <= sid <= 65535):
            raise ValueError('SID must fit in a 16-bit unsigned integer')

        self.sid = sid
        self.host = host
        self.port = port
        self.timeout = timeout

    def consume(self, timestamp=None):
        '''Yield all available transaction log entries starting at "timestamp".'''

        self.socket = socket.create_connection((self.host, self.port), self.timeout)

        start_ts = int(time.time() if timestamp is None else timestamp) * 10**9

        # Ask the server for all available transaction log entries since "start_ts"...
        self._write(struct.pack('!BIQH', MB_REPL, 0x00, start_ts, self.sid))

        magic, = struct.unpack('B', self._read(1))
        if magic != MB_REPL:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        while True:
            magic, ts = struct.unpack('!BQ', self._read(9))
            if magic == MB_SYNC:  # ...the head of the transaction log has been reached.
                self._write(struct.pack('B', MB_REPL))
                continue

            if magic != MB_REPL:
                raise KyotoTycoonException('bad response [%s]' % hex(magic))

            # Common log entry information...
            size, = struct.unpack('!I', self._read(4))
            sid, db, db_op = struct.unpack('!HHB', self._read(5))

            entry = {'sid': sid, 'db': db, 'opcode': db_op}

            buf = bytearray(self._read(size - 5))

            if sid == self.sid:  # ...this must never happen!
                raise KyotoTycoonException('bad log entry [sid=%d]' % sid)

            if db_op == OP_CLEAR:
                entry['operation'] = 'clear'

                yield entry
                continue

            if db_op == OP_REMOVE:
                entry['operation'] = 'remove'

                key_size, buf = self._read_varnum(buf)
                entry['key'] = bytes(buf[:key_size])

                yield entry
                continue

            if db_op == OP_SET:
                entry['operation'] = 'set'

                key_size, buf = self._read_varnum(buf)
                value_size, buf = self._read_varnum(buf)

                entry['key'] = bytes(buf[:key_size])
                entry['expires'], = struct.unpack('!Q', b'\x00\x00\x00' + bytes(buf[key_size:key_size+5]))
                entry['value'] = bytes(buf[key_size+5:key_size+value_size])

                yield entry
                continue

            raise KyotoTycoonException('unsupported database operation [%s]' % hex(db_op))

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        return True

    def _read_varnum(self, data):
        value = 0

        for i, byte in enumerate(data):
            value = (value << 7) + (byte & 0x7f)

            if byte < 0x80:
                return (value, data[i+1:])

        return (0, data)

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
