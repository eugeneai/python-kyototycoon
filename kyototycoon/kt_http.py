# -*- coding: utf-8 -*-
#
# Copyright 2011, Toru Maesaka
# Copyright 2013, Stephen Hamer
# Copyright 2013, Carlos Rodrigues
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#

import base64
import struct
import time
import sys

from .kt_error import KyotoTycoonException

from .kt_common import KT_PACKER_CUSTOM, \
                       KT_PACKER_PICKLE, \
                       KT_PACKER_JSON, \
                       KT_PACKER_STRING, \
                       KT_PACKER_BYTES

try:
    import httplib
except ImportError:
    import http.client as httplib

try:
    from urllib import quote as _quote
    from urllib import quote as _quote_from_bytes
    from urllib import unquote as unquote_to_bytes
except ImportError:
    from urllib.parse import quote as _quote
    from urllib.parse import quote_from_bytes as _quote_from_bytes
    from urllib.parse import unquote_to_bytes

quote = lambda s: _quote(s, safe='')
quote_from_bytes = lambda s: _quote_from_bytes(s, safe='')

try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

KT_HTTP_HEADER = {'Content-Type' : 'text/tab-separated-values; colenc=U'}

def _dict_to_tsv(kv_dict):
    lines = []
    for k, v in kv_dict.items():
        quoted = quote_from_bytes(v) if isinstance(v, bytes) else quote(str(v))
        lines.append('%s\t%s' % (quote(k.encode('utf-8')), quoted))
    return '\n'.join(lines)

def _content_type_decoder(content_type):
    '''Select the appropriate decoding function to use based on the response headers.'''

    if content_type.endswith('colenc=B'):
        return base64.decodestring

    if content_type.endswith('colenc=U'):
        return unquote_to_bytes

    return lambda x: x

def _tsv_to_dict(tsv_str, content_type):
    decode = _content_type_decoder(content_type)
    rv = {}

    for row in tsv_str.split(b'\n'):
        kv = row.split(b'\t')
        if len(kv) == 2:
            rv[decode(kv[0])] = decode(kv[1])
    return rv

def _tsv_to_list(tsv_str, content_type):
    decode = _content_type_decoder(content_type)
    rv = []

    for row in tsv_str.split(b'\n'):
        kv = row.split(b'\t')
        if len(kv) == 2:
            rv.append([decode(kv[0]), decode(kv[1])])
    return rv


class Cursor(object):
    cursor_id_counter = 1

    def __init__(self, protocol_handler):
        self.protocol_handler = protocol_handler
        self.cursor_id = Cursor.cursor_id_counter
        Cursor.cursor_id_counter += 1

        self.pack = self.protocol_handler.pack
        self.unpack = self.protocol_handler.unpack

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # Cleanup the cursor when leaving "with" blocks...
        self.delete()

    def __iter__(self):
        '''Return all (key,value) pairs for the cursor, in forward scan order.'''

        if not self.jump():
            return

        while True:
            yield self.get()

            if not self.step():
                return

    def jump(self, key=None, db=0):
        '''Jump the cursor to a record (first record if "None") for forward scan.'''

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/cur_jump?DB=' + db

        request_dict = {'CUR': self.cursor_id}
        if key:
            request_dict['key'] = key.encode('utf-8')

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status == 450:
            # Since this is normal while iterating, do not raise an exception...
            return False

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def jump_back(self, key=None, db=0):
        '''Jump the cursor to a record (last record if "None") for forward scan.'''

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/cur_jump_back?DB=' + db

        request_dict = {'CUR': self.cursor_id}
        if key:
            request_dict['key'] = key.encode('utf-8')

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status == 450:
            # Since this is normal while iterating, do not raise an exception...
            return False

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def step(self):
        '''Step the cursor to the next record.'''

        path = '/rpc/cur_step'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status == 450:
            # Since this is normal while iterating, do not raise an exception...
            return False

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def step_back(self):
        '''Step the cursor to the previous record.'''

        path = '/rpc/cur_step_back'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status == 450:
            # Since this is normal while iterating, do not raise an exception...
            return False

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def set_value(self, value, step=False, expire=None):
        '''Set the value for the current record.'''

        path = '/rpc/cur_set_value'
        request_dict = {'CUR': self.cursor_id, 'value': self.pack(value)}

        if step:
            request_dict['step'] = True

        if expire:
            request_dict['xt'] = expire

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def remove(self):
        '''Remove the current record.'''

        path = '/rpc/cur_remove'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def get_key(self, step=False):
        '''Get the key for the current record.'''

        path = '/rpc/cur_get_key'
        request_dict = {'CUR': self.cursor_id}

        if step:
            request_dict['step'] = True

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return _tsv_to_dict(body, res.getheader('Content-Type', ''))[b'key'].decode('utf-8')

    def get_value(self, step=False):
        '''Get the value for the current record.'''

        path = '/rpc/cur_get_value'
        request_dict = {'CUR': self.cursor_id}

        if step:
            request_dict['step'] = True

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return self.unpack(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'value'])

    def get(self, step=False):
        '''Get a (key,value) pair for the current record.'''

        path = '/rpc/cur_get'
        request_dict = {'CUR': self.cursor_id}

        if step:
            request_dict['step'] = True

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status == 404:
            return None, None

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        key = res_dict[b'key'].decode('utf-8')
        value = self.unpack(res_dict[b'value'])

        return key, value

    def seize(self):
        '''Get a (key,value) pair for the current record, and remove it atomically.'''

        path = '/rpc/cur_seize'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        seize_dict = {'key': res_dict[b'key'].decode('utf-8'),
                      'value': self.unpack(res_dict[b'value'])}

        return seize_dict

    def delete(self):
        '''Delete the cursor.'''

        path = '/rpc/cur_delete'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True


class ProtocolHandler(object):
    def __init__(self, pack_type=KT_PACKER_PICKLE, custom_packer=None):
        self.pack_type = pack_type

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
        return Cursor(self)

    def open(self, host, port, timeout):
        # Save connection parameters so the connection can be
        # re-established on a "Connection: close" response...
        self.host = host
        self.port = port
        self.timeout = timeout

        self.conn = httplib.HTTPConnection(host, port, timeout=timeout)
        return True

    def close(self):
        self.conn.close()
        return True

    def getresponse(self):
        res = self.conn.getresponse()
        body = res.read()

        if res.will_close:
            self.conn.close()
            self.open(self.host, self.port, self.timeout)

        return res, body

    def echo(self):
        self.conn.request('POST', '/rpc/echo')

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def get(self, key, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        self.conn.request('GET', path)
        res, body = self.getresponse()

        if res.status == 404:
            return None

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return self.unpack(body)

    def check(self, key, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/check?DB=' + db

        request_dict = {'key': key.encode('utf-8')}
        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status == 450:  # ...no record was found
            return False

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def seize(self, key, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/seize?DB=' + db

        request_dict = {'key': key.encode('utf-8')}
        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status == 450:  # ...no record was found
            return None

        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        return self.unpack(res_dict[b'value'])

    def set_bulk(self, kv_dict, expire, atomic, db=0):
        if isinstance(kv_dict, dict) and len(kv_dict) < 1:
            return 0  # ...done

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/set_bulk?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        if expire is not None:
            request_body.append('xt\t%d\n' % expire)

        for key, value in kv_dict.items():
            key = quote(key.encode('utf-8'))
            value = quote(self.pack(value))
            request_body.append('_%s\t%s\n' % (key, value))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        # Number of items set...
        return int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def remove_bulk(self, keys, atomic, db=0):
        if len(keys) < 1:
            return 0  # ...done

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/remove_bulk?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        for key in keys:
            request_body.append('_%s\t\n' % quote(key.encode('utf-8')))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        # Number of items removed...
        return int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def get_bulk(self, keys, atomic, db=0):
        if len(keys) < 1:
            return {}  # ...done

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/get_bulk?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        for key in keys:
            request_body.append('_%s\t\n' % quote(key.encode('utf-8')))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        rv = {}
        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        n = res_dict.pop(b'num')

        if n == '0':
            return {}

        for k, v in res_dict.items():
            if v is not None:
                rv[k.decode('utf-8')[1:]] = self.unpack(v)

        return rv

    def get_int(self, key, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        self.conn.request('GET', path)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return struct.unpack('>q', body)[0]

    def vacuum(self, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/vacuum?DB=' + db

        self.conn.request('GET', path)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def match_prefix(self, prefix, limit, db=0):
        if prefix is None:
            raise ValueError('no key prefix specified')

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/match_prefix?DB=' + db

        request_dict = {'prefix': prefix.encode('utf-8')}
        if limit:
            request_dict['max'] = limit

        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        rv = []
        res_list = _tsv_to_list(body, res.getheader('Content-Type', ''))
        if len(res_list) == 0 or res_list[-1][0] != b'num':
            raise KyotoTycoonException('server returned no data')

        num_key, num = res_list.pop()
        if num == '0':
            return []

        for k, v in res_list:
            rv.append(k.decode('utf-8')[1:])

        return rv

    def match_regex(self, regex, limit, db=0):
        if regex is None:
            raise ValueError('no regular expression specified')

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/match_regex?DB=' + db

        request_dict = {'regex': regex.encode('utf-8')}
        if limit:
            request_dict['max'] = limit

        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        rv = []
        res_list = _tsv_to_list(body, res.getheader('Content-Type', ''))
        if len(res_list) == 0 or res_list[-1][0] != b'num':
            raise KyotoTycoonException('server returned no data')

        num_key, num = res_list.pop()
        if num == '0':
            return []

        for k, v in res_list:
            rv.append(k.decode('utf-8')[1:])

        return rv

    def match_similar(self, origin, distance, limit, db=0):
        if origin is None:
            raise ValueError('no origin string specified')

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/match_similar?DB=' + db

        request_dict = {'origin': origin.encode('utf-8'), 'utf': ''}

        if distance is not None and distance >= 0:
            request_dict['range'] = distance

        if limit:
            request_dict['max'] = limit

        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        rv = []
        res_list = _tsv_to_list(body, res.getheader('Content-Type', ''))
        if len(res_list) == 0 or res_list[-1][0] != b'num':
            raise KyotoTycoonException('server returned no data')

        num_key, num = res_list.pop()
        if num == '0':
            return []

        for k, v in res_list:
            rv.append(k.decode('utf-8')[1:])

        return rv

    def set(self, key, value, expire, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        value = self.pack(value)
        status = self._rest_put(b'set', path, value, expire)
        if status != 201:
            raise KyotoTycoonException('protocol error [%d]' % status)

        return True

    def add(self, key, value, expire, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        value = self.pack(value)
        status = self._rest_put(b'add', path, value, expire)
        if status != 201:
            raise KyotoTycoonException('protocol error [%d]' % status)

        return True

    def cas(self, key, old_val, new_val, expire, db=0):
        if old_val is None and new_val is None:
            raise ValueError('old value and/or new value must be specified')

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/cas?DB=' + db

        request_dict = {'key': key.encode('utf-8')}

        if old_val is not None:
            request_dict['oval'] = self.pack(old_val)

        if new_val is not None:
            request_dict['nval'] = self.pack(new_val)

        if expire:
            request_dict['xt'] = expire

        request_body = _dict_to_tsv(request_dict)

        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def remove(self, key, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        self.conn.request('DELETE', path)

        res, body = self.getresponse()
        if res.status != 204:
            return False

        return True

    def replace(self, key, value, expire, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/%s/%s' % (db, quote(key.encode('utf-8')))

        value = self.pack(value)
        status = self._rest_put(b'replace', path, value, expire)
        if status != 201:
            return False

        return True

    def append(self, key, value, expire, db=0):
        # Simultaneous support for Python 2/3 makes this cumbersome...
        if sys.version_info[0] >= 3:
            bytes_type = bytes
            unicode_type = str
        else:
            bytes_type = str
            unicode_type = unicode

        if (not isinstance(value, bytes_type) and
            not isinstance(value, unicode_type)):
            raise ValueError('value is not a string or bytes type')

        old_data = self.get(key)
        data = type(value)() if old_data is None else old_data

        if (not isinstance(data, bytes_type) and
            not isinstance(data, unicode_type)):
            raise KyotoTycoonException('stored value is not a string or bytes type')

        if type(data) != type(value):
            if isinstance(data, bytes_type):
                value = value.encode('utf-8')
            else:
                value = value.decode('utf-8')

        data += value

        # This makes the operation atomic...
        if self.cas(key, old_data, data, expire, db) is not True:
            raise KyotoTycoonException('error while storing modified value')

        return True

    def increment(self, key, delta, expire, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/increment?DB=' + db

        request_body = 'key\t%s\nnum\t%d\n' % (key, delta)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def increment_double(self, key, delta, expire, db=0):
        if key is None:
            raise ValueError('no key specified')

        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/increment_double?DB=' + db

        request_body = 'key\t%s\nnum\t%f\n' % (key, delta)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return float(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def report(self):
        self.conn.request('GET', '/rpc/report')
        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        report_dict = {}
        for k, v in res_dict.items():
            report_dict[k.decode('utf-8')] = v.decode('utf-8')

        return report_dict

    def status(self, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/status?DB=' + db

        self.conn.request('GET', path)
        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        status_dict = {}
        for k, v in res_dict.items():
            status_dict[k.decode('utf-8')] = v.decode('utf-8')

        return status_dict

    def clear(self, db=0):
        db = str(db) if isinstance(db, int) else quote(db.encode('utf-8'))
        path = '/rpc/clear?DB=' + db

        self.conn.request('GET', path)
        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        return True

    def count(self, db=0):
        st = self.status(db)
        return None if st is None else int(st['count'])

    def size(self, db=0):
        st = self.status(db)
        return None if st is None else int(st['size'])

    def play_script(self, name, kv_dict=None):
        if kv_dict is None:
            kv_dict = {}

        path = '/rpc/play_script?name=' + quote(name.encode('utf-8'))

        request_body = []
        for k, v in kv_dict.items():
            if not isinstance(v, bytes):
                raise ValueError('value must be a byte sequence')

            k = quote(k.encode('utf-8'))
            v = quote(v)
            request_body.append('_%s\t%s\n' % (k, v))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            raise KyotoTycoonException('protocol error [%d]' % res.status)

        rv = {}
        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))

        for k, v in res_dict.items():
            if v is not None:
                rv[k.decode('utf-8')[1:]] = v

        return rv

    def _rest_put(self, operation, key, value, expire):
        headers = {b'X-Kt-Mode' : operation}
        if expire is not None:
            headers[b'X-Kt-Xt'] = str(int(time.time()) + expire)

        self.conn.request('PUT', key, value, headers)
        res, body = self.getresponse()
        return res.status

# EOF - kt_http.py
