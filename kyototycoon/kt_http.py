#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011, Toru Maesaka
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.

import base64
import struct
import time
import sys

from . import kt_error

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

try:
    import simplejson as json
except ImportError:
    import json

KT_HTTP_HEADER = {'Content-Type' : 'text/tab-separated-values; colenc=U'}

KT_PACKER_CUSTOM = 0
KT_PACKER_PICKLE = 1
KT_PACKER_JSON   = 2
KT_PACKER_STRING = 3

def _dict_to_tsv(dict):
    lines = []
    for k, v in dict.items():
        quoted = quote_from_bytes(v) if isinstance(v, bytes) else quote(str(v))
        lines.append('%s\t%s' % (quote(k.encode('utf-8')), quoted))
    return '\n'.join(lines)

def _content_type_decoder(content_type=''):
    '''Select the appropriate decoding function to use based on the response headers.'''

    if content_type.endswith('colenc=B'):
        return base64.decodestring

    if content_type.endswith('colenc=U'):
        return unquote_to_bytes

    return lambda x: x

def _tsv_to_dict(tsv_str, content_type=''):
    decode = _content_type_decoder(content_type)
    rv = {}

    for row in tsv_str.split(b'\n'):
        kv = row.split(b'\t')
        if len(kv) == 2:
            rv[decode(kv[0])] = decode(kv[1])
    return rv

def _tsv_to_list(tsv_str, content_type=''):
    decode = _content_type_decoder(content_type)
    rv = []

    for row in tsv_str.split(b'\n'):
        kv = row.split(b'\t')
        if len(kv) == 2:
            pair = (decode(kv[0]), decode(kv[1]))
            rv.append(pair)
    return rv


class Cursor(object):
    cursor_id_counter = 1

    def __init__(self, protocol_handler):
        self.protocol_handler = protocol_handler
        self.cursor_id = Cursor.cursor_id_counter
        Cursor.cursor_id_counter += 1

        self.err = kt_error.KyotoTycoonError()
        self.pack = self.protocol_handler.pack
        self.unpack = self.protocol_handler.unpack

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        # Cleanup the cursor when leaving "with" blocks...
        self.delete()

    def jump(self, key=None, db=None):
        '''Jump the cursor to a record (first record if "None") for forward scan.'''

        path = '/rpc/cur_jump'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path = '%s?DB=%s' % (path, db)

        request_dict = {'CUR': self.cursor_id}
        if key:
            request_dict['key'] = quote(key.encode('utf-8'))

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def jump_back(self, key=None, db=None):
        '''Jump the cursor to a record (last record if "None") for forward scan.'''

        path = '/rpc/cur_jump_back'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path = '%s?DB=%s' % (path, db)

        request_dict = {'CUR': self.cursor_id}
        if key:
            request_dict['key'] = quote(key.encode('utf-8'))

        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def step(self):
        '''Step the cursor to the next record.'''

        path = '/rpc/cur_step'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def step_back(self):
        '''Step the cursor to the previous record.'''

        path = '/rpc/cur_step_back'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
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
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def remove(self):
        '''Remove the current record.'''

        path = '/rpc/cur_remove'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
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
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
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
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
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
            self.err.set_error(self.err.NOTFOUND)
            return None, None

        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False, False

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        key = res_dict[b'key'].decode('utf-8')
        value = self.unpack(res_dict[b'value'])

        self.err.set_success()
        return key, value

    def seize(self):
        '''Get a (key,value) pair for the current record, and remove it atomically.'''

        path = '/rpc/cur_seize'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        seize_dict = {'key': res_dict[b'key'].decode('utf-8'),
                      'value': self.unpack(res_dict[b'value'])}

        self.err.set_success()
        return seize_dict

    def delete(self):
        '''Delete the cursor.'''

        path = '/rpc/cur_delete'
        request_dict = {'CUR': self.cursor_id}
        request_body = _dict_to_tsv(request_dict)
        self.protocol_handler.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.protocol_handler.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True


class ProtocolHandler(object):
    def __init__(self, pack_type=KT_PACKER_PICKLE, pickle_protocol=2):
        self.err = kt_error.KyotoTycoonError()
        self.pack_type = pack_type

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
           self.err.set_error(self.err.EMISC)
           return False

        self.err.set_success()
        return True

    def get(self, key, db=None):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        self.conn.request('GET', key)
        res, body = self.getresponse()

        if res.status == 404:
            self.err.set_error(self.err.NOTFOUND)
            return None

        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return self.unpack(body)

    def set_bulk(self, kv_dict, expire, atomic, db):
        if isinstance(kv_dict, dict) and len(kv_dict) < 1:
            self.err.set_error(self.err.LOGIC)
            return 0

        path = '/rpc/set_bulk'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        for k, v in kv_dict.items():
            k = quote(k.encode('utf-8'))
            v = quote(self.pack(v))
            request_body.append('_%s\t%s\n' % (k, v))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def remove_bulk(self, keys, atomic, db):
        if len(keys) < 1:
            self.err.set_error(self.err.LOGIC)
            return 0

        path = '/rpc/remove_bulk'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        for key in keys:
            request_body.append('_%s\t\n' % quote(key.encode('utf-8')))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        count = int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])
        if count > 0:
            self.err.set_success()
        else:
            self.err.set_error(self.err.NOTFOUND)

        return count

    def get_bulk(self, keys, atomic, db):
        if len(keys) < 1:
            self.err.set_error(self.err.LOGIC)
            return {}

        path = '/rpc/get_bulk'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_body = ['atomic\t\n' if atomic else '']

        for key in keys:
            request_body.append('_%s\t\n' % quote(key.encode('utf-8')))

        self.conn.request('POST', path, body=''.join(request_body), headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        rv = {}
        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        n = res_dict.pop(b'num')

        if n == '0':
            self.err.set_error(self.err.NOTFOUND)
            return {}

        for k, v in res_dict.items():
            if v is not None:
                rv[k.decode('utf-8')[1:]] = self.unpack(v)

        self.err.set_success()
        return rv

    def get_int(self, key, db=None):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        self.conn.request('GET', key)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.NOTFOUND)
            return None

        self.err.set_success()
        return struct.unpack('>q', body)[0]

    def vacuum(self, db):
        path = '/rpc/vacuum'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        self.conn.request('GET', path)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def match_prefix(self, prefix, max, db):
        rv = []
        request_dict = {'prefix': prefix.encode('utf-8')}

        if max:
            request_dict['max'] = max

        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            request_dict['DB'] = db

        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', '/rpc/match_prefix', body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        res_list = _tsv_to_list(body, res.getheader('Content-Type', ''))
        if len(res_list) == 0 or res_list[-1][0] != b'num':
            self.err.set_error(self.err.EMISC)
            return False
        num_key, num = res_list.pop()
        if num == '0':
            self.err.set_error(self.err.NOTFOUND)
            return []

        for k, v in res_list:
            rv.append(k.decode('utf-8')[1:])

        self.err.set_success()
        return rv

    def match_regex(self, regex, max, db):
        if regex is None:
            self.err.set_error(self.err.LOGIC)
            return None

        path = '/rpc/match_regex'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_dict = {'regex': regex.encode('utf-8')}
        if max:
            request_dict['max'] = max

        request_body = _dict_to_tsv(request_dict)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        rv = []
        res_list = _tsv_to_list(body, res.getheader('Content-Type', ''))
        if len(res_list) == 0 or res_list[-1][0] != b'num':
            self.err.set_error(self.err.EMISC)
            return False
        num_key, num = res_list.pop()
        if num == '0':
            self.err.set_error(self.err.NOTFOUND)
            return []

        for k, v in res_list:
            rv.append(k.decode('utf-8')[1:])

        self.err.set_success()
        return rv

    def set(self, key, value, expire, db):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        value = self.pack(value)
        status = self._rest_put('set', key, value, expire)
        if status != 201:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def add(self, key, value, expire, db):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        value = self.pack(value)
        status = self._rest_put('add', key, value, expire)
        if status != 201:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def cas(self, key, old_val, new_val, expire, db):
        if old_val is None and new_val is None:
            self.err.set_error(self.err.LOGIC)
            return False

        path = '/rpc/cas'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_dict = {'key': quote(key.encode('utf-8'))}

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
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def remove(self, key, db):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        self.conn.request('DELETE', key)

        res, body = self.getresponse()
        if res.status != 204:
            self.err.set_error(self.err.NOTFOUND)
            return False

        self.err.set_success()
        return True

    def replace(self, key, value, expire, db):
        key = quote(key.encode('utf-8'))
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            key = '/%s/%s' % (db, key)

        value = self.pack(value)
        status = self._rest_put('replace', key, value, expire)
        if status != 201:
            self.err.set_error(self.err.NOTFOUND)
            return False

        self.err.set_success()
        return True

    def append(self, key, value, expire, db):
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

        data = self.get(key)
        if data is None:
            data = type(value)()

        if (not isinstance(data, bytes_type) and
            not isinstance(data, unicode_type)):
            self.err.set_error(self.err.EMISC)
            return False

        if type(data) != type(value):
            if isinstance(data, bytes_type):
                value = value.encode('utf-8')
            else:
                value = value.decode('utf-8')

        data += value

        if self.set(key, data, expire, db) is not True:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def increment(self, key, delta, expire, db):
        path = '/rpc/increment'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_body = 'key\t%s\nnum\t%d\n' % (key, delta)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return None

        self.err.set_success()
        return int(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def increment_double(self, key, delta, expire, db):
        if key is None:
            self.err.set_error(self.err.LOGIC)
            return False

        path = '/rpc/increment_double'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        request_body = 'key\t%s\nnum\t%f\n' % (key, delta)
        self.conn.request('POST', path, body=request_body, headers=KT_HTTP_HEADER)

        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return None

        self.err.set_success()
        return float(_tsv_to_dict(body, res.getheader('Content-Type', ''))[b'num'])

    def report(self):
        self.conn.request('GET', '/rpc/report')
        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return None

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        report_dict = {}
        for k, v in res_dict.items():
            report_dict[k.decode('utf-8')] = v.decode('utf-8')

        self.err.set_success()
        return report_dict

    def status(self, db=None):
        path = '/rpc/status'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        self.conn.request('GET', path)
        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return None

        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))
        status_dict = {}
        for k, v in res_dict.items():
            status_dict[k.decode('utf-8')] = v.decode('utf-8')

        self.err.set_success()
        return status_dict

    def clear(self, db=None):
        path = '/rpc/clear'
        if db:
            db = db if isinstance(db, int) else quote(db.encode('utf-8'))
            path += '?DB=' + db

        self.conn.request('GET', path)
        res, body = self.getresponse()
        if res.status != 200:
            self.err.set_error(self.err.EMISC)
            return False

        self.err.set_success()
        return True

    def count(self, db=None):
        st = self.status(db)
        return None if st is None else int(st['count'])

    def size(self, db=None):
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
            self.err.set_error(self.err.LOGIC)
            return None

        rv = {}
        res_dict = _tsv_to_dict(body, res.getheader('Content-Type', ''))

        for k, v in res_dict.items():
            if v is not None:
                rv[k.decode('utf-8')[1:]] = v

        self.err.set_success()
        return rv

    def _rest_put(self, operation, key, value, expire):
        headers = {'X-Kt-Mode' : operation}
        if expire is not None:
            headers["X-Kt-Xt"] = str(int(time.time()) + expire)

        self.conn.request('PUT', key, value, headers)
        res, body = self.getresponse()
        return res.status

# vim: set expandtab ts=4 sw=4
