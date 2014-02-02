#!/usr/bin/env python
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
import time

from . import kt_error

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import simplejson as json
except ImportError:
    import json

KT_PACKER_CUSTOM = 0
KT_PACKER_PICKLE = 1
KT_PACKER_JSON   = 2
KT_PACKER_STRING = 3

MB_SET_BULK = 0xb8
MB_GET_BULK = 0xba
MB_REMOVE_BULK = 0xb9
MB_PLAY_SCRIPT = 0xb4

# Maximum signed 64bit integer...
DEFAULT_EXPIRE = 0x7fffffffffffffff

class ProtocolHandler(object):
    def __init__(self, pack_type=KT_PACKER_PICKLE, pickle_protocol=2):
        self.err = kt_error.KyotoTycoonError()
        self.socket = None

        if pack_type == KT_PACKER_PICKLE:
            self.pack = lambda data: pickle.dumps(data, pickle_protocol)
            self.unpack = lambda data: pickle.loads(data)
        elif pack_type == KT_PACKER_JSON:
            self.pack = lambda data: json.dumps(data, separators=(',',':')).encode('utf-8')
            self.unpack = lambda data: json.loads(data.decode('utf-8'))
        elif pack_type == KT_PACKER_STRING:
            self.pack = lambda data: data.encode('utf-8')
            self.unpack = lambda data: data.decode('utf-8')
        else:
            raise Exception('unsupported pack type specified')

    def error(self):
        return self.err

    def cursor(self):
        raise NotImplementedError('supported under the HTTP procotol only')

    def open(self, host, port, timeout):
        self.socket = socket.create_connection((host, port), timeout)
        return True

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        return True

    def get(self, key, db=None):
        values = self.get_bulk([key], True, db)
        return values[key] if values else None

    def set_bulk(self, kv_dict, expire, atomic, db):
        if isinstance(kv_dict, dict) and len(kv_dict) < 1:
            self.err.set_error(self.err.LOGIC)
            return 0

        expire = DEFAULT_EXPIRE if expire is None else int(time.time()) + expire

        if db is None:
            db = 0

        request = [struct.pack('!BII', MB_SET_BULK, 0, len(kv_dict))]

        for key, value in kv_dict.items():
            key = key.encode('utf-8')
            value = self.pack(value)
            request.extend([struct.pack('!HIIq', db, len(key), len(value), expire), key, value])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_SET_BULK:
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))
        self.err.set_success()
        return num_items

    def remove_bulk(self, keys, atomic, db):
        if len(keys) < 1:
            self.err.set_error(self.err.LOGIC)
            return 0

        if db is None:
            db = 0

        request = [struct.pack('!BII', MB_REMOVE_BULK, 0, len(keys))]

        for key in keys:
            key = key.encode('utf-8')
            request.extend([struct.pack('!HI', db, len(key)), key])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_REMOVE_BULK:
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))
        if num_items > 0:
            self.err.set_success()
        else:
            self.err.set_error(self.err.NOTFOUND)

        return num_items

    def get_bulk(self, keys, atomic, db):
        if len(keys) < 1:
            self.err.set_error(self.err.LOGIC)
            return {}

        if db is None:
            db = 0

        request = [struct.pack('!BII', MB_GET_BULK, 0, len(keys))]

        for key in keys:
            key = key.encode('utf-8')
            request.extend([struct.pack('!HI', db, len(key)), key])

        self._write(b''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_GET_BULK:
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))
        items = {}
        for i in range(num_items):
            key_db, key_length, value_length, key_expire = struct.unpack('!HIIq', self._read(18))
            key = self._read(key_length)
            value = self._read(value_length)
            items[key.decode('utf-8')] = self.unpack(value)

        if num_items > 0:
            self.err.set_success()
        else:
            self.err.set_error(self.err.NOTFOUND)

        return items

    def get_int(self, key, db=None):
        raise NotImplementedError('supported under the HTTP procotol only')

    def vacuum(self, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def match_prefix(self, prefix, max, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def match_regex(self, regex, max, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def set(self, key, value, expire, db):
        numitems = self.set_bulk({key: value}, expire, True, db)
        return numitems > 0

    def add(self, key, value, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def cas(self, key, old_val, new_val, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def remove(self, key, db):
        numitems = self.remove_bulk([key], True, db)
        return numitems > 0

    def replace(self, key, value, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def append(self, key, value, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def increment(self, key, delta, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def increment_double(self, key, delta, expire, db):
        raise NotImplementedError('supported under the HTTP procotol only')

    def report(self):
        raise NotImplementedError('supported under the HTTP procotol only')

    def status(self, db=None):
        raise NotImplementedError('supported under the HTTP procotol only')

    def clear(self, db=None):
        raise NotImplementedError('supported under the HTTP procotol only')

    def count(self, db=None):
        raise NotImplementedError('supported under the HTTP procotol only')

    def size(self, db=None):
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
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))
        items = {}
        for i in range(num_items):
            key_length, value_length = struct.unpack('!II', self._read(8))
            key = self._read(key_length)
            value = self._read(value_length)
            items[key.decode('utf-8')] = value

        self.err.set_success()
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

# vim: set expandtab ts=4 sw=4
