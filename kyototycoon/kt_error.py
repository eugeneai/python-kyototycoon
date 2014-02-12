# -*- coding: utf-8 -*-
#
# Copyright 2011, Toru Maesaka
#
# Redistribution and use of this source code is licensed under
# the BSD license. See COPYING file for license description.

class KyotoTycoonException(Exception):
    pass

class KyotoTycoonError(object):
    SUCCESS  = 0
    NOIMPL   = 1
    INVALID  = 2
    LOGIC    = 3
    INTERNAL = 4
    NETWORK  = 5
    NOTFOUND = 6
    EMISC    = 255

    ErrorNameDict = {
        SUCCESS: "SUCCESS",
        NOIMPL: "UNIMPLEMENTED",
        INVALID: "INVALID",
        LOGIC: "LOGIC",
        INTERNAL: "INTERNAL",
        NETWORK: "NETWORK",
        NOTFOUND: "NOTFOUND",
        EMISC: "EMISC",
    }

    ErrorMessageDict = {
        SUCCESS: "Operation Successful",
        NOIMPL: "Unimplemented Operation",
        INVALID: "Invalid Operation",
        LOGIC: "Logic Error",
        INTERNAL: "Internal Error",
        NETWORK: "Network Error",
        NOTFOUND: "Record Not Found",
        EMISC: "Miscellaneous Error",
    }

    def __init__(self, exceptions=False):
        '''
        Initialize the last database operation error object.

        The "exceptions" parameter controls whether an exception is raised when
        the library sets the error state to something that's an actual error.

        '''

        self.exceptions = exceptions
        self.set_success()

    def set_error(self, code, detail_message=None):
        '''Set the error state for the last database operation.'''

        self.error_code = code
        self.error_name = self.ErrorNameDict[code]
        self.error_message = self.ErrorMessageDict[code]
        self.error_detail = detail_message

        if self.exceptions and not self.ok():
            raise KyotoTycoonException(self.message())

    def set_success(self):
        '''Flag the last database operation as having been successful.'''

        self.set_error(self.SUCCESS)

    def ok(self):
        '''Return whether the last database operation resulted in an error.'''

        return self.error_code in (self.SUCCESS, self.NOTFOUND)

    def code(self):
        '''Return the error code for the last database operation.'''

        return self.error_code

    def name(self):
        '''Return the error name for the last database operation.'''

        return self.error_name

    def message(self):
        '''Return the error description for the last database operation.'''

        if self.error_detail:
            return '%s (%s)' % (self.error_message, self.error_detail)
        else:
            return self.error_message

# EOF - kt_error.py
