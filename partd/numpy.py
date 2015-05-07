from __future__ import absolute_import

import numpy as np
import struct
from . import core

destroy = core.destroy

def create(path, dtype):
    """ Create store with known dtype """
    core.create(path)
    with open(core.filename(path, '.dtype'), 'w') as f:
        f.write(str(dtype))


def dtype(path):
    with open(core.filename(path, '.dtype')) as f:
        text = f.read()
    return np.dtype(eval(text))  # TODO: this is insecure


def put(path, data, put=core.put, **kwargs):
    """ Put dict of numpy arrays into store """
    bytes = dict((k, v.tobytes()) for k, v in data.items())
    put(path, bytes, **kwargs)


def get(path, keys, get=core.get, **kwargs):
    """ Get list of numpy arrays from store """
    bytes = get(path, keys, **kwargs)
    dt = dtype(path)
    return [np.frombuffer(bytes[i], dtype=dt[k]) for i, k in enumerate(keys)]
