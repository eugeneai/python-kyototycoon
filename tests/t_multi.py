#!/usr/bin/env python
#
# Copyright 2011, Toru Maesaka
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.
#
# Kyoto Tycoon should be started like this:
#   $ ktserver one.kch two.kch three.kch

import config
import time
import unittest
from kyototycoon import KyotoTycoon

DB_1 = 'one.kch'
DB_2 = 'two.kch'
DB_INVALID = 'invalid.kch'

class UnitTest(unittest.TestCase):
    def setUp(self):
        self.kt_handle = KyotoTycoon()
        self.kt_handle.open()
        self.LARGE_KEY_LEN = 8000

    def clear_all(self):
        self.assertTrue(self.kt_handle.clear(db=DB_1))
        self.assertTrue(self.kt_handle.clear(db=DB_2))
        return True

    def test_status(self):
        status = self.kt_handle.status(DB_1)
        assert status is not None

        status = self.kt_handle.status(DB_2)
        assert status is not None

        status = self.kt_handle.status(DB_INVALID)
        assert status is None

        status = self.kt_handle.status('non_existent')
        assert status is None

    def test_set_get(self):
        self.assertTrue(self.clear_all())
        self.assertTrue(self.kt_handle.set('ice', 'cream', db=DB_2))
        self.assertFalse(self.kt_handle.set('palo', 'alto', db=DB_INVALID))

        assert self.kt_handle.get('ice') is None
        assert self.kt_handle.get('ice', db=DB_1) is None
        assert self.kt_handle.get('ice', db=DB_INVALID) is None

        self.assertEqual(self.kt_handle.get('ice', db='two.kch'), 'cream')
        self.assertEqual(self.kt_handle.count(db=DB_1), 0)
        self.assertEqual(self.kt_handle.count(db=DB_2), 1)

        self.assertTrue(self.kt_handle.set('frozen', 'yoghurt', db=DB_1))
        self.assertEqual(self.kt_handle.count(db=DB_1), 1)

        self.assertEqual(self.kt_handle.get('frozen'), 'yoghurt')
        self.assertEqual(self.kt_handle.get('frozen', db=DB_1), 'yoghurt')
        assert self.kt_handle.get('frozen', db=DB_2) is None
        assert self.kt_handle.get('frozen', db=DB_INVALID) is None

        self.assertTrue(self.kt_handle.clear(db=DB_1))
        self.assertEqual(self.kt_handle.count(db=DB_1), 0)
        self.assertEqual(self.kt_handle.count(db=DB_2), 1)

    def test_set_get_int(self):
        self.assertTrue(self.clear_all())
        self.assertTrue(self.kt_handle.set_int('key1', 1, db=DB_1))
        self.assertTrue(self.kt_handle.set_int('key2', 2, db=DB_2))
        self.assertFalse(self.kt_handle.set_int('key3', 3, db=DB_INVALID))

        self.assertEqual(self.kt_handle.get_int('key1'), 1)
        self.assertEqual(self.kt_handle.get_int('key1', db=0), 1)
        self.assertEqual(self.kt_handle.get_int('key1', db=DB_1), 1)
        assert self.kt_handle.get('key1', db=DB_2) is None
        assert self.kt_handle.get('key1', db=DB_INVALID) is None

        self.assertEqual(self.kt_handle.get_int('key2', db=DB_2), 2)
        self.assertEqual(self.kt_handle.get_int('key2', db=1), 2)
        assert self.kt_handle.get('key2', db=DB_1) is None
        assert self.kt_handle.get('key2', db=DB_INVALID) is None
        assert self.kt_handle.get('key1', db=DB_2) is None

    def test_add(self):
        self.assertTrue(self.clear_all())

        # Should not conflict due to different databases.
        self.assertTrue(self.kt_handle.add('key1', 'val1', db=DB_1))
        self.assertTrue(self.kt_handle.add('key1', 'val1', db=DB_2))

        # Now they should.
        self.assertFalse(self.kt_handle.add('key1', 'val1', db=DB_1))
        self.assertFalse(self.kt_handle.add('key1', 'val1', db=DB_2))
        self.assertFalse(self.kt_handle.add('key1', 'val1', db=DB_INVALID))

    def test_replace(self):
        self.assertTrue(self.clear_all())
        self.assertTrue(self.kt_handle.add('key1', 'val1', db=DB_1))
        self.assertFalse(self.kt_handle.replace('key1', 'val2', db=DB_2))
        self.assertTrue(self.kt_handle.replace('key1', 'val2', db=DB_1))
        self.assertFalse(self.kt_handle.replace('key1', 'val2', db=DB_INVALID))

        self.assertTrue(self.kt_handle.add('key2', 'aaa'))
        self.assertTrue(self.kt_handle.replace('key2', 'bbb'))
        self.assertTrue(self.kt_handle.replace('key1', 'zzz'))
        self.assertEqual(self.kt_handle.get('key2'), 'bbb')
        self.assertEqual(self.kt_handle.get('key1'), 'zzz')

    def test_remove(self):
        self.assertTrue(self.clear_all())
        self.assertTrue(self.kt_handle.add('key', 'value', db=DB_1))
        self.assertTrue(self.kt_handle.add('key', 'value', db=DB_2))

        self.assertTrue(self.kt_handle.remove('key', db=DB_1))
        self.assertEqual(self.kt_handle.get('key', db=DB_2), 'value')
        assert self.kt_handle.get('key', db=DB_1) is None

    def test_vacuum(self):
        self.assertTrue(self.kt_handle.vacuum())
        self.assertTrue(self.kt_handle.vacuum(db=DB_1))
        self.assertTrue(self.kt_handle.vacuum(db=DB_2))
        self.assertFalse(self.kt_handle.vacuum(db=DB_INVALID))

    def test_append(self):
        self.assertTrue(self.clear_all())
        self.assertTrue(self.kt_handle.set('key', 'xxx', db=DB_1))
        self.assertTrue(self.kt_handle.set('key', 'xxx', db=DB_2))
        self.assertTrue(self.kt_handle.append('key', 'xxx', db=DB_1))

        self.assertEqual(self.kt_handle.get('key', db=DB_1), 'xxxxxx')
        self.assertEqual(self.kt_handle.get('key', db=DB_2), 'xxx')

    def test_increment(self):
        self.assertTrue(self.clear_all())
        self.assertEqual(self.kt_handle.increment('key', 0, db=DB_1), 0)
        self.assertEqual(self.kt_handle.increment('key', 0, db=DB_2), 0)

        self.assertEqual(self.kt_handle.increment('key', 100, db=DB_1), 100)
        self.assertEqual(self.kt_handle.increment('key', 200, db=DB_2), 200)
        self.assertEqual(self.kt_handle.increment('key', 100, db=DB_1), 200)
        self.assertEqual(self.kt_handle.increment('key', 200, db=DB_2), 400)
        self.assertEqual(self.kt_handle.get_int('key', db=DB_1), 200)
        self.assertEqual(self.kt_handle.get_int('key', db=DB_2), 400)

if __name__ == '__main__':
    unittest.main()