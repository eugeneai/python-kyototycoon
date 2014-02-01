#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011, Toru Maesaka
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#
# Note that python-kyototycoon follows the following interface
# standard: http://fallabs.com/kyototycoon/kyototycoon.idl

from . import kt_http
from . import kt_binary

KT_DEFAULT_HOST = '127.0.0.1'
KT_DEFAULT_PORT = 1978
KT_DEFAULT_TIMEOUT = 30

class KyotoTycoon(object):
    def __init__(self, binary=False, *args, **kwargs):
        '''Initialize a "Binary Protocol" or "HTTP" KyotoTycoon object.'''

        protocol = kt_binary if binary else kt_http
        self.core = protocol.ProtocolHandler(*args, **kwargs)

    def error(self):
        '''Return the error state from the last operation.'''

        return self.core.error()

    def open(self, host=KT_DEFAULT_HOST, port=KT_DEFAULT_PORT, timeout=KT_DEFAULT_TIMEOUT):
        '''Open a new connection to a KT server.'''

        return self.core.open(host, port, timeout)

    def close(self):
        '''Close an open connection to the KT server.'''

        return self.core.close()

    def report(self):
        '''Get a server information report.'''

        return self.core.report()

    def status(self, db=None):
        '''Get status information for the database.'''

        return self.core.status(db)

    def clear(self, db=None):
        '''Remove all records in the database.'''

        return self.core.clear(db)

    def count(self, db=None):
        '''Number of records in the database.'''

        return self.core.count(db)

    def size(self, db=None):
        '''Current database size (in bytes).'''

        return self.core.size(db)

    def set(self, key, value, expire=None, db=None):
        '''Set the value for a record.'''

        return self.core.set(key, value, expire, db)

    def add(self, key, value, expire=None, db=None):
        '''Set the value for a record (does nothing if the record already exists).'''

        return self.core.add(key, value, expire, db)

    def replace(self, key, value, expire=None, db=None):
        '''Replace the value of an existing record.'''

        return self.core.replace(key, value, expire, db)

    def append(self, key, value, expire=None, db=None):
        '''Append "value" to the string value of a record.'''

        return self.core.append(key, value, expire, db)

    def increment(self, key, delta, expire=None, db=None):
        '''Add "delta" to the numeric integer value of a record.'''

        return self.core.increment(key, delta, expire, db)

    def increment_double(self, key, delta, expire=None, db=None):
        '''Add "delta" to the numeric double value of a record.'''

        return self.core.increment_double(key, delta, expire, db)

    def cas(self, key, old_val=None, new_val=None, expire=None, db=None):
        '''If the old value of a record is "old_val", replace it with "new_val".'''

        return self.core.cas(key, old_val, new_val, expire, db)

    def remove(self, key, db=None):
        '''Remove a record.'''

        return self.core.remove(key, db)

    def get(self, key, db=None):
        '''Retrieve the value for a record.'''

        return self.core.get(key, db)

    def get_int(self, key, db=None):
        '''Retrieve the numeric integer value for a record.'''

        return self.core.get_int(key, db)

    def set_bulk(self, kv_dict, expire=None, atomic=True, db=None):
        '''Set the values for several records at once.'''

        return self.core.set_bulk(kv_dict, expire, atomic, db)

    def remove_bulk(self, keys, atomic=True, db=None):
        '''Remove several records at once.'''

        return self.core.remove_bulk(keys, atomic, db)

    def get_bulk(self, keys, atomic=True, db=None):
        '''Retrieve the values for several records at once.'''

        return self.core.get_bulk(keys, atomic, db)

    def vacuum(self, db=None):
        '''Scan the database and eliminate regions of expired records.'''

        return self.core.vacuum(db)

    def match_prefix(self, prefix, max=None, db=None):
        '''Get keys matching a prefix string.'''

        return self.core.match_prefix(prefix, max, db)

    def match_regex(self, regex, max=None, db=None):
        '''Get keys matching a ragular expression string.'''

        return self.core.match_regex(regex, max, db)

    def cursor(self):
        '''Obtain a new (uninitialized) record cursor.'''

        return self.core.cursor()

    def play_script(self, name, kv_dict=None):
        '''
        Call a procedure of the scripting language extension.

        Because the input/output of server-side scripts may use a mix of formats, and unlike all
        other methods, no implicit packing/unpacking is done to either input or output values.

        '''

        return self.core.play_script(name, kv_dict)

# vim: set expandtab ts=4 sw=4
