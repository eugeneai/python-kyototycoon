# -*- coding: utf-8 -*-
#
# Copyright 2013, Carlos Rodrigues
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#
# This is based on Ulrich Mierendorff's code, originally at:
#  - http://www.ulrichmierendorff.com/software/kyoto_tycoon/python_library.html
#

import socket
import struct

from .kt_error import KyotoTycoonException

from .kt_common import KT_PACKER_CUSTOM, \
                       KT_PACKER_PICKLE, \
                       KT_PACKER_JSON, \
                       KT_PACKER_STRING, \
                       KT_PACKER_BYTES

try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

MB_SET_BULK = 0xb8
MB_GET_BULK = 0xba
MB_REMOVE_BULK = 0xb9
MB_PLAY_SCRIPT = 0xb4

# Maximum signed 64bit integer...
DEFAULT_EXPIRE = 0x7fffffffffffffff

class ProtocolHandler(object):
    def __init__(self, pack_type=KT_PACKER_PICKLE, custom_packer=None):
        self.socket = None

        if pack_type != KT_PACKER_CUSTOM and custom_packer is not None:
            raise KyotoTycoonException('custom packer object supported for "KT_PACKER_CUSTOM" only')

        if pack_type == KT_PACKER_PICKLE:
            # Pickle protocol v2 is is used here instead of the default...
            self.pack = lambda data: pickle.dumps(data, 2)
            self.unpack = lambda data: pickle.loads(data)

        elif pack_type == KT_PACKER_JSON:
            self.pack = lambda data: json.dumps(data, separators=(',',':')).encode('utf-8')
            self.unpack = lambda data: json.loads(data.decode('utf-8'))

        elif pack_type == KT_PACKER_STRING:
            self.pack = lambda data: data.encode('utf-8')
            self.unpack = lambda data: data.decode('utf-8')

        elif pack_type == KT_PACKER_BYTES:
            self.pack = lambda data: data
            self.unpack = lambda data: data

        elif pack_type == KT_PACKER_CUSTOM:
            if custom_packer is None:
                raise KyotoTycoonException('"KT_PACKER_CUSTOM" requires a packer object')

            self.pack = custom_packer.pack
            self.unpack = custom_packer.unpack

        else:
            raise KyotoTycoonException('unsupported pack type specified')

    def cursor(self):
        raise NotImplementedError('supported under the HTTP procotol only')

    def open(self, host, port, timeout):
        self.socket = socket.create_connection((host, port), timeout)
        return True

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        return True

    def get(self, key, db=0):
        values = self.get_bulk([key], False, db)

        # This should never occur, but it does. What's happening?
        if values and key not in values:
            raise KyotoTycoonException('key mismatch: ' + repr(values))

        return values[key] if values else None

    def check(self, key, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def seize(self, key, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def set_bulk(self, kv_dict, expire, atomic, db=0):
        if atomic:
            raise KyotoTycoonException('atomic supported under the HTTP procotol only')

        if isinstance(kv_dict, dict) and len(kv_dict) < 1:
            return 0  # ...done

        if expire is None:
            expire = DEFAULT_EXPIRE

        request = [struct.pack('!BII', MB_SET_BULK, 0, len(kv_dict))]

        for key, value in kv_dict.items():
            key = key.encode('utf-8')
            value = self.pack(value)
            request.extend([struct.pack('!HIIq', db, len(key), len(value), expire), key, value])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_SET_BULK:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        # Number of items set...
        return struct.unpack('!I', self._read(4))[0]

    def remove_bulk(self, keys, atomic, db=0):
        if atomic:
            raise KyotoTycoonException('atomic supported under the HTTP procotol only')

        if len(keys) < 1:
            return 0  # ...done

        request = [struct.pack('!BII', MB_REMOVE_BULK, 0, len(keys))]

        for key in keys:
            key = key.encode('utf-8')
            request.extend([struct.pack('!HI', db, len(key)), key])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_REMOVE_BULK:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        # Number of items removed...
        return struct.unpack('!I', self._read(4))[0]

    def get_bulk(self, keys, atomic, db=0):
        if atomic:
            raise KyotoTycoonException('atomic supported under the HTTP procotol only')

        if len(keys) < 1:
            return {}  # ...done

        request = [struct.pack('!BII', MB_GET_BULK, 0, len(keys))]

        for key in keys:
            key = key.encode('utf-8')
            request.extend([struct.pack('!HI', db, len(key)), key])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_GET_BULK:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        num_items, = struct.unpack('!I', self._read(4))
        items = {}
        for i in range(num_items):
            key_db, key_length, value_length, key_expire = struct.unpack('!HIIq', self._read(18))
            key = self._read(key_length)
            value = self._read(value_length)
            items[key.decode('utf-8')] = self.unpack(value)

        return items

    def get_int(self, key, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def vacuum(self, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def match_prefix(self, prefix, limit, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def match_regex(self, regex, limit, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def match_similar(self, origin, distance, limit, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def set(self, key, value, expire, db=0):
        numitems = self.set_bulk({key: value}, expire, False, db)
        return numitems > 0

    def add(self, key, value, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def cas(self, key, old_val, new_val, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def remove(self, key, db=0):
        numitems = self.remove_bulk([key], False, db)
        return numitems > 0

    def replace(self, key, value, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def append(self, key, value, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def increment(self, key, delta, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def increment_double(self, key, delta, expire, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def report(self):
        raise NotImplementedError('supported under the HTTP procotol only')

    def status(self, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def clear(self, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def count(self, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def size(self, db=0):
        raise NotImplementedError('supported under the HTTP procotol only')

    def play_script(self, name, kv_dict=None):
        if kv_dict is None:
            kv_dict = {}

        name = name.encode('utf-8')
        request = [struct.pack('!BIII', MB_PLAY_SCRIPT, 0, len(name), len(kv_dict)), name]

        for key, value in kv_dict.items():
            if not isinstance(value, bytes):
                raise ValueError('value must be a byte sequence')

            key = key.encode('utf-8')
            request.extend([struct.pack('!II', len(key), len(value)), key, value])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_PLAY_SCRIPT:
            raise KyotoTycoonException('bad response [%s]' % hex(magic))

        num_items, = struct.unpack('!I', self._read(4))
        items = {}
        for i in range(num_items):
            key_length, value_length = struct.unpack('!II', self._read(8))
            key = self._read(key_length)
            value = self._read(value_length)
            items[key.decode('utf-8')] = value

        return items

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

# EOF - kt_binary.py
