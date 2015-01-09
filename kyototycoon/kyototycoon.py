# -*- coding: utf-8 -*-
#
# Copyright 2011, Toru Maesaka
# Copyright 2013, Stephen Hamer
# Copyright 2013, Carlos Rodrigues
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#

import warnings

from . import kt_http
from . import kt_binary

from .kt_common import KT_PACKER_PICKLE

class KyotoTycoon(object):
    def __init__(self, binary=False, pack_type=KT_PACKER_PICKLE,
                       custom_packer=None, exceptions=True):
        '''
        Initialize a "Binary Protocol" or "HTTP Protocol" KyotoTycoon object.

        Note: The default packer uses pickle protocol v2, which is the highest
              version that's still compatible with both Python 2 and 3. If you
              require a different version, specify a custom packer object.

        '''

        if not exceptions:
            # Relying on separate error states is bad form and should be avoided...
            raise DeprecationWarning('not raising exceptions on error has been removed')

        if binary:
            self.atomic = False  # The binary protocol does not support atomic operations.
            self.core = kt_binary.ProtocolHandler(pack_type, custom_packer)
        else:
            self.atomic = True
            self.core = kt_http.ProtocolHandler(pack_type, custom_packer)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self, host='127.0.0.1', port=1978, timeout=30):
        '''Open a new connection to a KT server.'''

        return True if self.core.open(host, port, timeout) else False

    def connect(self, *args, **kwargs):
        '''
        Open a new connection to a KT server.

        The same as "open()" but returning "self" instead of a boolean, allowing
        KyotoTycoon objects to be used as context managers in "with" statements.

        '''

        return self if self.open(*args, **kwargs) else None

    def close(self):
        '''Close an open connection to the KT server.'''

        return self.core.close()

    def report(self):
        '''Get a server information report.'''

        return self.core.report()

    def status(self, db=0):
        '''Get status information for the database.'''

        return self.core.status(db)

    def clear(self, db=0):
        '''Remove all records in the database.'''

        return self.core.clear(db)

    def count(self, db=0):
        '''Number of records in the database.'''

        return self.core.count(db)

    def size(self, db=0):
        '''Current database size (in bytes).'''

        return self.core.size(db)

    def set(self, key, value, expire=None, db=0):
        '''Set the value for a record.'''

        return self.core.set(key, value, expire, db)

    def add(self, key, value, expire=None, db=0):
        '''Set the value for a record (does nothing if the record already exists).'''

        return self.core.add(key, value, expire, db)

    def replace(self, key, value, expire=None, db=0):
        '''Replace the value of an existing record.'''

        return self.core.replace(key, value, expire, db)

    def append(self, key, value, expire=None, db=0):
        '''Append "value" to the string value of a record.'''

        return self.core.append(key, value, expire, db)

    def increment(self, key, delta, expire=None, db=0):
        '''Add "delta" to the numeric integer value of a record.'''

        return self.core.increment(key, delta, expire, db)

    def increment_double(self, key, delta, expire=None, db=0):
        '''Add "delta" to the numeric double value of a record.'''

        return self.core.increment_double(key, delta, expire, db)

    def cas(self, key, old_val=None, new_val=None, expire=None, db=0):
        '''If the old value of a record is "old_val", replace it with "new_val".'''

        return self.core.cas(key, old_val, new_val, expire, db)

    def remove(self, key, db=0):
        '''Remove a record.'''

        return self.core.remove(key, db)

    def get(self, key, db=0):
        '''Retrieve the value for a record.'''

        return self.core.get(key, db)

    def check(self, key, db=0):
        '''Check that a record exists in the database.'''

        return self.core.check(key, db)

    def seize(self, key, db=0):
        '''Retrieve the value for a record and immediately remove it.'''

        return self.core.seize(key, db)

    def get_int(self, key, db=0):
        '''Retrieve the numeric integer value for a record.'''

        return self.core.get_int(key, db)

    def set_bulk(self, kv_dict, expire=None, atomic=None, db=0):
        '''Set the values for several records at once.'''

        return self.core.set_bulk(kv_dict, expire, self.atomic if atomic is None else atomic, db)

    def remove_bulk(self, keys, atomic=None, db=0):
        '''Remove several records at once.'''

        return self.core.remove_bulk(keys, self.atomic if atomic is None else atomic, db)

    def get_bulk(self, keys, atomic=None, db=0):
        '''Retrieve the values for several records at once.'''

        return self.core.get_bulk(keys, self.atomic if atomic is None else atomic, db)

    def vacuum(self, db=0):
        '''Scan the database and eliminate regions of expired records.'''

        return self.core.vacuum(db)

    def match_prefix(self, prefix, limit=None, db=0):
        '''Get keys matching a prefix string.'''

        return self.core.match_prefix(prefix, limit, db)

    def match_regex(self, regex, limit=None, db=0):
        '''Get keys matching a ragular expression string.'''

        return self.core.match_regex(regex, limit, db)

    def match_similar(self, origin, distance=0, limit=None, db=0):
        '''Get keys similar to the origin string, based on the levenshtein distance.'''

        return self.core.match_similar(origin, distance, limit, db)

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

# EOF - kyototycoon.py
