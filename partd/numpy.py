from __future__ import absolute_import

""" Store arrays

We put arrays on disk as raw bytes, extending along the first dimension.
Alongside each array x we ensure the value x.dtype which stores the string
description of the array's dtype.
"""

import numpy as np
from functools import partial
import struct
from contextlib import contextmanager
try:
    from cytoolz import memoize
except ImportError:
    from toolz import memoize
from . import core

destroy = core.destroy
create = core.create
partd = core.partd

def extend(key, term):
    """

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


@memoize(key=lambda args, kwargs: (args[0], tuple(args[1])))
def dtypes(path, keys, get=core.get, lock=False, **kwargs):
    dt_keys = [extend(key, '.dtype') for key in keys]
    text = get(path, dt_keys, lock=lock, **kwargs)
    return list(map(parse_dtype, text))


def parse_dtype(s):
    """

    >>> parse_dtype('int32')
    dtype('int32')

    >>> parse_dtype("[('a', 'int32')]")
    dtype([('a', '<i4')])
    """
    if '[' in s:
        return np.dtype(eval(s))  # Dangerous!
    else:
        return np.dtype(s)


def put(path, data, put=core.put, **kwargs):
    """ Put dict of numpy arrays into store """
    bytes = dict((k, v.tobytes()) for k, v in data.items())
    put(path, bytes, **kwargs)
    for k, v in data.items():
        core.ensure(path, extend(k, '.dtype'), str(v.dtype))


def get(path, keys, get=core.get, **kwargs):
    """ Get list of numpy arrays from store """
    bytes = get(path, keys, **kwargs)
    dts = dtypes(path, keys, get=get, **kwargs)
    return list(map(np.frombuffer, bytes, dts))
