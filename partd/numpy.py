""" Store arrays

We put arrays on disk as raw bytes, extending along the first dimension.
Alongside each array x we ensure the value x.dtype which stores the string
description of the array's dtype.
"""
from __future__ import absolute_import
import numpy as np


def extend(key, term):
    """ Extend a key with a suffix

    Works if they key is a string or a tuple

    >>> extend('x', '.dtype')
    'x.dtype'
    >>> extend(('a', 'b', 'c'), '.dtype')
    ('a', 'b', 'c.dtype')
    """
    if isinstance(key, str):
        return key + term
    elif isinstance(key, tuple):
        return key[:-1] + (extend(key[-1], term),)
    else:
        return extend(str(key), term)


def parse_dtype(s):
    """ Parse text as numpy dtype

    >>> parse_dtype('int32')
    dtype('int32')

    >>> parse_dtype("[('a', 'int32')]")
    dtype([('a', '<i4')])
    """
    if b'[' in s:
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
            self.partd.iset(extend(k, '.dtype'), str(v.dtype).encode())
        self.partd.append(valmap(np.ndarray.tobytes, data), **kwargs)

    def _get(self, keys, **kwargs):
        bytes = self.partd._get(keys, **kwargs)
        dtypes = self.partd._get([extend(key, '.dtype') for key in keys],
                                 lock=False)
        dtypes = map(parse_dtype, dtypes)
        return list(map(np.frombuffer, bytes, dtypes))

    def delete(self, keys, **kwargs):
        keys2 = [extend(key, '.dtype') for key in keys]
        self.partd.delete(keys2, **kwargs)

    def _iset(self, key, value):
        return self.partd._iset(key, value)

    def drop(self):
        return self.partd.drop()

    @property
    def lock(self):
        return self.partd.lock
