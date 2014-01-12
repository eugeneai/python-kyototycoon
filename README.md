About
-----

Python client library for Kyoto Tycoon. For more information on
Kyoto Tycoon please refer to the official project website:

  * http://fallabs.com/kyototycoon/

This library's interface follows the preferred interface
provided by Kyoto Tycoon's original author(s):

  * http://fallabs.com/kyototycoon/kyototycoon.idl

There is currently no documentation for this software but in
the meantime please see "kyototycoon.py" for the interface.

This fork is backwards compatible, and includes cursor support
and significant performance improvements over the original
library.

The more efficient binary protocol is also supported along with
the HTTP protocol, for a performance improvement of around **6x**.
Only a minimal subset of operations are supported for this
protocol, namely:

  * "get()" and "get_bulk()"
  * "set()" and "set_bulk()"
  * "remove()" and "remove_bulk()"

Currently, the "play_script()" operation is not implemented for
either protocol choices.

Installation
------------

Using pip:

    pip install python-kyototycoon

Or, from source:

    python setup.py build
    sudo python setup.py install


Known Issues
------------

  * When using the HTTP protocol and "KT_PACKER_STRING", if
    values are unicode strings then the "set()" method is
    about **20x** slower when compared to non-unicode strings.

  * With Python 2.6, all HTTP operations are about **20x**
    slower than Python 2.7 (tested on a Debian machine).
    There is no such performance difference between 2.6/2.7
    when using the binary protocol.


Authors and Thanks
------------------

  * Toru Maesaka <dev@torum.net>
  * Stephen Hamer <stephen.hamer@upverter.com>

Binary protocol support was added by Carlos Rodrigues based
on Ulrich Mierendorff's code with only minimal changes to make
it fit this library. You can find the original library at the
following URL:

  * http://www.ulrichmierendorff.com/software/kyoto_tycoon/python_library.html
