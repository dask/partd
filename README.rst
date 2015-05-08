PartD
=====

Minimal key-value byte storage with appendable values

Partd stores data.  Partd arranges data in key-value pairs where the values are
just raw bytes.  We can append data onto the end of existing values.  You can
get the entire value associated to a list of keys.


API
---

Partd supports four operations

1.  ``create``: Create a partd dataset::

        >>> create('/path/to/partd-file/')

2.  ``put``:  Append key-byte pairs to dataset::

        >>> put('/path/to/partd-file/', {'x': b'Hello ', 'y': b'123'})
        >>> put('/path/to/partd-file/', {'x': b'world!', 'y': b'456'})

3.  ``get``:  Get all bytes associated to a set of keys::

        >>> get('/path/to/partd-file/', ['y', 'x'])
        [b'123456', b'Hello world!']

4.  ``destroy``:  Destroy partd dataset::

        >>> destroy('/path/to/partd-file')

That's it.

There is no in-memory state.

Implementations
---------------

The reference implementation uses file-based locks.  This works surprisingly
well as long as you don't do many small writes

If you do many small writes then you probably want to cache in memory; this is
hard to do in parallel while also maintaining consistency.  For this we have a
centralized server (see ``partd.zmq``) that caches data in memory and writes
only large chunks to disk when necessary.


Encodings
---------

``partd.pickle`` and ``partd.numpy`` handle encoding/decoding pickled lists and
numpy arrays with the same ``partd`` ``get/put`` interface.  They compose well
with the zmq solution.


Future plans
------------

Things that will happen if our plan is perfect (it isn't!)

1.  These functions can be implemented in other, non-file-based ways, perhaps
    with a distributed system that peer-to-peer communication and stuff.
2.  We'll build interfaces around partd that handle dataframes, numpy arrays,
    pickled lists, etc..
3.  Compression
