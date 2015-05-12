from contextlib import contextmanager
import os
import shutil
import tempfile
import struct


def raises(exc, lamda):
    try:
        lamda()
        return False
    except exc:
        return True


@contextmanager
def tmpfile(extension=''):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension)
    os.close(handle)
    os.remove(filename)

    try:
        yield filename
    finally:
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                os.remove(filename)


def frame(bytes):
    """ Pack the length of the bytes in front of the bytes

    TODO: This does a full copy.  This should maybe be inlined somehow
    whereever this gets used instead.  My laptop shows a data bandwidth of
    2GB/s
    """
    return struct.pack('Q', len(bytes)) + bytes


def framesplit(bytes):
    """ Split buffer into frames of concatenated chunks

    >>> data = frame(b'Hello') + frame(b'World')
    >>> list(framesplit(data))  # doctest: +SKIP
    [b'Hello', b'World']
    """
    i = 0; n = len(bytes)
    chunks = list()
    while i < n:
        nbytes = struct.unpack('Q', bytes[i:i+8])[0]
        i += 8
        yield bytes[i: i + nbytes]
        i += nbytes


def partition_all(n, bytes):
    """

    >>> list(partition_all(2, b'Hello'))
    ['He', 'll', 'o']
    """
    if len(bytes) < n:  # zero copy fast common case
        yield bytes
    else:
        for i in range(0, len(bytes), n):
            yield bytes[i: i+n]
