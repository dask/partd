PartD
=====

Append-only key-value storage.

Partd stores data.  Partd arranges data in key-value pairs where the keys are
small and the values are long and appendable bytestrings.  You can add new data
onto the end of existing values.  You can get the entire value associated to a
list of keys.

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

4.  ``destroy``:  Destroy up partd dataset::

        >>> destroy('/path/to/partd-file')

That's it.

There is no in-memory state.  Partd coordinates between many processes using
file-based locks.


Future plans
------------

Things that will happen if our plan is exactly perfect (it isn't!)

1.  These functions can be implemented in other, non-file-based ways, perhaps
    with a distributed system that does in-memory buffering and peer-to-peer
    communication and stuff.
2.  We'll build interfaces around partd that handle dataframes, numpy arrays,
    pickled lists, etc..
