""" Store arrays

We put arrays on disk as raw bytes, extending along the first dimension.
Alongside each array x we ensure the value x.dtype which stores the string
description of the array's dtype.
"""
from __future__ import absolute_import
import numpy as np
from .compatibility import pickle
from .utils import frame, framesplit, suffix, ignoring

def serialize_dtype(dt):
    """ Serialize dtype to bytes

    >>> serialize_dtype(np.dtype('i4'))
    '<i4'
    >>> serialize_dtype(np.dtype('M8[us]'))
    '<M8[us]'
    """
    return dt.str


def parse_dtype(s):
    """ Parse text as numpy dtype

    >>> parse_dtype('i4')
    dtype('int32')

    >>> parse_dtype("[('a', 'i4')]")
    dtype([('a', '<i4')])
    """
    if s.startswith(b'['):
        return np.dtype(eval(s))  # Dangerous!
    else:
        return np.dtype(s)


from .core import Interface
from .file import File
from toolz import valmap


class Numpy(Interface):
    def __init__(self, partd):
        if isinstance(partd, str):
            partd = File(partd)
        self.partd = partd
        Interface.__init__(self)

    def __getstate__(self):
        return {'partd': self.partd}

    def append(self, data, **kwargs):
        for k, v in data.items():
            self.partd.iset(suffix(k, '.dtype'), serialize_dtype(v.dtype))
        self.partd.append(valmap(serialize, data), **kwargs)

    def _get(self, keys, **kwargs):
        bytes = self.partd._get(keys, **kwargs)
        dtypes = self.partd._get([suffix(key, '.dtype') for key in keys],
                                 lock=False)
        dtypes = map(parse_dtype, dtypes)
        return list(map(deserialize, bytes, dtypes))

    def delete(self, keys, **kwargs):
        keys2 = [suffix(key, '.dtype') for key in keys]
        self.partd.delete(keys2, **kwargs)

    def _iset(self, key, value):
        return self.partd._iset(key, value)

    def drop(self):
        return self.partd.drop()

    @property
    def lock(self):
        return self.partd.lock

    def __exit__(self, *args):
        self.drop()
        self.partd.__exit__(self, *args)

try:
    from pandas import msgpack
except ImportError:
    try:
        import msgpack
    except ImportError:
        msgpack = False


def serialize(x):
    if x.dtype == 'O':
        with ignoring(Exception):  # Try msgpack (faster on strings)
            return frame(msgpack.packb(x.tolist()))
        return frame(pickle.dumps(x.tolist(), protocol=pickle.HIGHEST_PROTOCOL))
    else:
        return x.tobytes()


def deserialize(bytes, dtype, copy=False):
    if dtype == 'O':
        try:
            lists = list(map(msgpack.unpackb, framesplit(bytes)))
        except:
            lists = list(map(pickle.loads, framesplit(bytes)))

        return np.array(sum(lists, []), dtype='O')
    else:
        result = np.frombuffer(bytes, dtype)
        if copy:
            result = result.copy()
        return result


import snappy, blosc
blosc.set_nthreads(1)


def compress(bytes, dtype):
    if dtype == 'O':
        return snappy.compress(bytes)
    else:
        return blosc.compress(bytes, dtype.itemsize)


def decompress(bytes, dtype):
    if dtype == 'O':
        return snappy.decompress(bytes)
    else:
        return blosc.decompress(bytes)
