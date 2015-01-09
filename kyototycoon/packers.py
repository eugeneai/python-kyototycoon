# -*- coding: utf-8 -*-
#
# Copyright 2015, Carlos Rodrigues
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#

import struct
import gzip

try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

class SimpleMemcachePacker(object):
    '''
    Kyoto Tycoon servers supporting the memcached protocol store the item flags (if enabled)
    within the data itself. This packer marshalls "(value, flags)" pairs in this scenario.

    '''

    def pack(self, data):
        '''Pack a "(value, flags)" pair, where "value" is a sequence of bytes.'''

        if not (0 <= data[1] < 2**32):
            raise ValueError('flags must fit in a 32-bit unsigned integer')

        return data[0] + struct.pack('>I', data[1])

    def unpack(self, data):
        '''Return a "(value, flags)" pair, where "value" is a sequence of bytes.'''

        return (data[:-4], struct.unpack('>I', data[-4:])[0])

class MemcachePacker(object):
    '''
    Kyoto Tycoon servers supporting the memcached protocol store the item flags (if enabled)
    within the data itself. This packer takes care of marshalling byte data in this scenario.

    Note: Flags are ignored on read and *zeroed* on write. The only exception to this is the
          gzip flag, which triggers decompression when gzip is enabled. When compressing data
          on write, the gzip flag will also be set (but all other flag bits are still zeroed).

          If you need control over the flags, use the "SimpleMemcachePacker" class instead.

    '''

    def __init__(self, gzip_enabled=False, gzip_threshold=128, gzip_flag=1):
        '''Initialize the packer object with optional gzip compression.'''

        self.gzip_enabled = gzip_enabled
        self.gzip_threshold = gzip_threshold
        self.gzip_flag = gzip_flag

        if not (1 <= self.gzip_flag <= 32):
            raise ValueError('gzip flag must be an integer between 1 and 32')

        # Flags are stored as 32-bit unsigned integers (big-endian)...
        self.zero_flag_bytes = b'\x00\x00\x00\x00'
        self.gzip_flag_bytes = struct.pack('>I', 0x1 << (self.gzip_flag - 1))

    def pack(self, data):
        '''Pack the (byte) data, optionally compressing it.'''

        # Don't compress data if it's too small or compression is disabled...
        if not self.gzip_enabled or len(data) < self.gzip_threshold:
            return data + self.zero_flag_bytes

        sio = StringIO()
        gz = gzip.GzipFile(fileobj=sio, mode='w')
        gz.write(data)
        gz.close()
        gzip_data = sio.getvalue()
        sio.close()

        return gzip_data + self.gzip_flag_bytes

    def unpack(self, data):
        '''Unpack the (byte) data, decompressing when required.'''

        stored_data = data[:-4]
        stored_flags, = struct.unpack('>I', data[-4:])

        # If compression is enabled, check if the data needs decompression...
        if self.gzip_enabled and stored_flags & 0x1 << (self.gzip_flag - 1):
            sio = StringIO(stored_data)
            gz = gzip.GzipFile(fileobj=sio, mode='r')
            stored_data = gz.read()
            gz.close()
            sio.close()

        return stored_data

# EOF - packers.py
