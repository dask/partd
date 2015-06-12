PartD
=====

|Build Status|

Minimal key-value byte storage with appendable values

Partd stores key-value pairs.
The values are raw bytes.
We append onto existing values.

Partd is useful for shuffling operations.


API
---


1.  Create a Partd::

        >>> import partd
        >>> p = partd.File('/path/to/new/dataset/'')

2.  Append key-byte pairs to dataset::

        >>> p.append({'x': b'Hello ', 'y': b'123'})
        >>> p.append({'x': b'world!', 'y': b'456'})

3.  Get all bytes associated to a set of keys::

        >>> p.get(['y', 'x'])
        [b'123456', b'Hello world!']

4.  Idempotently set single key-value pair (no append, no update)::

        >>> p.iset('z', b'metadata'])

4.  Destroy partd dataset::

        >>> p.drop()

That's it.

There is no in-memory state.

Implementations
---------------

The reference implementation uses file-based locks.  This works surprisingly
well as long as you don't do many small writes.

If you do many small writes then you probably want to cache in memory.
Fortunately you can buffer one partd with another, writing only large chunks as
necessary when spaces runs low::

    >>> p = Buffer(Dict(), File(), available_memory=2e9)  # 2GB memory buffer

You might also want to have many distributed process write to a single partd
consistently.  This can be done with a server

*   Server Process::

        >>> local_partd = File()
        >>> p = Server(local_partd, address='ipc://server')

*   Worker processes::

        >>> p = Client('ipc://server')
        >>> p.append(...)


Encodings and Compression
-------------------------

Once we can robustly and efficiently append bytes we move on to encoding
various things as bytes either with serialization systems like Pickle or
MSGPack or with compression routines like zlib, snappy, or blosc.  In principle
we want to compose all of these choice together

1.  Write policy:  ``Dict``, ``File``, ``Buffer``, ``Client``
2.  Encoding:  ``Pickle``, ``Numpy``, ``Pandas``, ...
3.  Compression:  ``Blosc``, ``Snappy``, ...

Partd objects compose by nesting for example here we make a server backed by a
buffered dict/file combination with a client using snappy compressed pickle
data::

    >>> server = Server(Buffer(Dict(), File(), available_memory=2e0))

    >>> client = Pickle(Snappy(Client(server.address)))

And here a partd that writes pickle encoded BZ2 compressed bytes directly to
disk::

    >>> p = Pickle(BZ2(File('foo')))

.. |Build Status| image:: https://travis-ci.org/mrocklin/partd.png
   :target: https://travis-ci.org/mrocklin/partd
