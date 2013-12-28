#!/usr/bin/env python
#
# Copyright 2011, Toru Maesaka
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
import kt_error

try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

KT_PACKER_CUSTOM = 0
KT_PACKER_PICKLE = 1
KT_PACKER_JSON   = 2
KT_PACKER_STRING = 3

MB_SET_BULK = 0xb8
MB_GET_BULK = 0xba
MB_REMOVE_BULK = 0xb9

# Maximum signed 64bit integer...
DEFAULT_EXPIRE = 0x7fffffffffffffff

class ProtocolHandler(object):
    def __init__(self, pack_type=KT_PACKER_PICKLE, pickle_protocol=2):
        self.err = kt_error.KyotoTycoonError()
        self.pickle_protocol = pickle_protocol
        self.pack_type = pack_type

        if self.pack_type == KT_PACKER_PICKLE:
            self.pack = self._pickle_packer
            self.unpack = self._pickle_unpacker

        elif self.pack_type == KT_PACKER_JSON:
            self.pack = self._json_packer
            self.unpack = self._json_unpacker

        elif self.pack_type == KT_PACKER_STRING:
            self.pack = lambda data: data
            self.unpack = lambda data: data

        else:
            raise Exception('unknown pack type specified')

    def error(self):
        return self.err

    def cursor(self):
        raise NotImplementedError

    def open(self, host, port, timeout):
        self.socket = socket.create_connection((host, port), timeout)
        return True

    def close(self):
        self.socket.close()
        return True

    def getresponse(self):
        raise NotImplementedError

    def echo(self):
        raise NotImplementedError

    def get(self, key, db=None):
        if key is None:
            return False

        values = self.get_bulk([key], True, db)
        if not values:
            return None

        return values[key]

    def set_bulk(self, kv_dict, expire, atomic, db):
        if not isinstance(kv_dict, dict):
            return False

        if len(kv_dict) < 1:
            self.err.set_error(self.err.LOGIC)
            return False

        if db is None:
            db = 0

        if expire is None:
            expire = DEFAULT_EXPIRE

        request = [struct.pack('!BI', MB_SET_BULK, 0), struct.pack('!I', len(kv_dict))]

        for key, value in kv_dict.iteritems():
            value = self.pack(value)
            request.append(struct.pack('!HIIq', db, len(key), len(value), expire))
            request.append(key)
            request.append(value)

        self._write(''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_SET_BULK:
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))

        self.err.set_success()
        return num_items

    def remove_bulk(self, keys, atomic, db):
        raise NotImplementedError

    def get_bulk(self, keys, atomic, db):
        if not hasattr(keys, '__iter__'):
            self.err.set_error(self.err.LOGIC)
            return None

        if db is None:
            db = 0

        request = [struct.pack('!BI', MB_GET_BULK, 0), struct.pack('!I', len(keys))]

        for key in keys:
            request.append(struct.pack('!HI', db, len(key)))
            request.append(key)

        self._write(''.join(request))

        magic, = struct.unpack('!B', self._read(1))
        if magic != MB_GET_BULK:
            self.err.set_error(self.err.INTERNAL)
            return False

        num_items, = struct.unpack('!I', self._read(4))
        items = {}
        for i in xrange(num_items):
            key_db, key_length, value_length, key_expire = struct.unpack('!HIIq', self._read(18))
            key = self._read(key_length)
            value = self._read(value_length)
            items[key] = self.unpack(value)
        return items

    def get_int(self, key, db=None):
        raise NotImplementedError

    def vacuum(self, db):
        raise NotImplementedError

    def match_prefix(self, prefix, max, db):
        raise NotImplementedError

    def match_regex(self, regex, max, db):
        raise NotImplementedError

    def set(self, key, value, expire, db):
        return self.set_bulk({key: value}, expire, True, db)

    def add(self, key, value, expire, db):
        raise NotImplementedError

    def cas(self, key, old_val, new_val, expire, db):
        raise NotImplementedError

    def remove(self, key, db):
        raise NotImplementedError

    def replace(self, key, value, expire, db):
        raise NotImplementedError

    def append(self, key, value, expire, db):
        raise NotImplementedError

    def increment(self, key, delta, expire, db):
        raise NotImplementedError

    def increment_double(self, key, delta, expire, db):
        raise NotImplementedError

    def report(self):
        raise NotImplementedError

    def status(self, db=None):
        raise NotImplementedError

    def clear(self, db=None):
        raise NotImplementedError

    def count(self, db=None):
        raise NotImplementedError

    def size(self, db=None):
        raise NotImplementedError

    def _pickle_packer(self, data):
        if type(data) is str:
            return data
        return pickle.dumps(data, self.pickle_protocol)

    def _pickle_unpacker(self, data):
        try:
            res = pickle.loads(data)
        except EOFError as err:
            res = ""
        except ValueError as err:
            res = data
        except pickle.UnpicklingError as err:
            if type(data) is str:
                res = data
            else:
                raise
        return res

    def _json_packer(self, data):
        return json.dumps(data)

    def _json_unpacker(self, data):
        return json.loads(data)

    def _write(self, data):
        self.socket.sendall(data)

    def _read(self, bytecnt):
        buf = []
        read = 0
        while read < bytecnt:
            recv = self.socket.recv(bytecnt-read)
            if recv:
                buf.append(recv)
                read += len(recv)

        return ''.join(buf)

