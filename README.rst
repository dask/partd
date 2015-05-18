PartD
=====

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

If you do many small writes then you probably want to cache in memory; this is
hard to do in parallel while also maintaining consistency.  For this we have a
centralized server (see ``partd.Shared``) that caches data in memory and writes
only large chunks to disk when necessary

*   Server Process::

        >>> server = p.Server('/path/to/dataset', 'ipc://server')

*   Worker processes::

        >>> p = Shared('ipc://server')
        >>> p.append(...)


Encodings and Compression
-------------------------

Once we can robustly and efficiently append bytes we move on to encoding
various things as bytes either with serialization systems like Pickle or
MSGPack or with compression routines like zlib, snappy, or blosc.  In principle
we want to compose all of these choice together

1.  Write policy:  ``partd.File``, ``partd.Shared``
2.  Encoding:  ``partd.Pickle``, ``partd.Numpy``
3.  Compression:  ``partd.Blosc``, ``partd.Snappy``, ...

Partd objects compose by nesting for example here we make a shared
server that writes snappy compressed numpy arrays::

    >>> p = partd.Numpy(partd.Snappy(partd.Shared('foo')))

And here a partd that writes pickle encoded BZ2 compressed bytes directly to
disk::

    >>> p = partd.Pickle(partd.BZ2(partd.File('foo')))
