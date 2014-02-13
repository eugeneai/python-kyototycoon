ABOUT
-----
This is a native Python client library for Kyoto Tycoon.
For more information on Kyoto Tycoon please refer to the
official project website:

  http://fallabs.com/kyototycoon/

This library's interface follows the preferred interface
provided by Kyoto Tycoon's original author(s):

  http://fallabs.com/kyototycoon/kyototycoon.idl

There is currently no documentation for this software but, in
the meantime, please see the file "kyototycoon.py" for the
interface.

This library includes cursor support and significant performance
improvements over the (now unmaintained) original library by
Toru Maesaka. It also supports unicode keys and works with both
Python 2 and 3.

The more efficient binary protocol is also supported along with
the HTTP protocol. It provides a performance improvement of up
to 6x, but only the following operations are available:

  * ``get()`` and ``get_bulk()``
  * ``set()`` and ``set_bulk()``
  * ``remove()`` and ``remove_bulk()``
  * ``play_script()``

Atomic operations aren't supported with the binary protocol,
the use of "atomic=False" is mandatory when using it. Operations
besides these will raise a ``NotImplementedError`` exception.

It's possible to have two KyotoTycoon objects open to the same
server in the same application, one using HTTP and the other
using the binary protocol, if necessary.

The library does automatic packing and unpacking (marshalling)
of values coming from/to the database. The following data
storage formats are available by default:

  * ``KT_PACKER_PICKLE`` - Python "pickle" format.
  * ``KT_PACKER_JSON`` - JSON format (compact representation).
  * ``KT_PACKER_STRING`` - Strings (UTF-8).
  * ``KT_PACKER_BYTES`` - Binary data.

There is also a ``KT_PACKER_CUSTOM`` format available where you
can specify your own object to do the marshalling. This object
needs to provide the following two methods:

  * ``.pack(self, data)`` - convert "data" to ``bytes()``
  * ``.unpack(self, data)`` - convert "data" from ``bytes()``

Marshalling is done for all methods except ``play_script()``,
because the server can return data in more than one format at
once. The caller will most likely know the type of data that
the called script returns and must do the marshalling itself.


INSTALLATION
------------
You can install the latest version of this library from source::

    python setup.py build
    python setup.py install

A version of this library is available from PyPI, although it may
not always be the latest and greatest version. You can find it at:

  https://pypi.python.org/pypi/python-kyototycoon/

You can install packages directly from PyPI using ``pip``::

    pip install python-kyototycoon

This library is still not at version 1.0, which means the API and
behavior are not guaranteed to remain consistent between versions.
If you require a specific version consider using versioning with
``pip``. For example::

    pip install python-kyototycoon==0.4.6

This is ideal for use with any automatic scripts you may be using
to build/deploy your application.


AUTHORS
-------
  * Toru Maesaka <dev@torum.net>
  * Stephen Hamer <stephen.hamer@upverter.com>
  * Carlos Rodrigues <cefrodrigues@gmail.com>

Binary protocol support was added based on Ulrich Mierendorff's
code with only minimal changes to make it fit this library.
You can find the original library at the following URL:

  http://www.ulrichmierendorff.com/software/kyoto_tycoon/python_library.html
