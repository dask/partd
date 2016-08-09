"""
get/put functions that consume/produce Python lists using msgpack or pickle
to serialize.

First we try msgpack (it's faster).  If that fails then we default to pickle.
"""
from __future__ import absolute_import
from .compatibility import pickle

try:
    from pandas import msgpack
except ImportError:
    try:
        import msgpack
    except ImportError:
        msgpack = False


from .encode import Encode
from functools import partial


def dumps(x):
    try:
        return msgpack.packb(x, use_bin_type=True)
    except:
        return pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)

def loads(x):
    try:
        return msgpack.unpackb(x, encoding='utf8')
    except:
        return pickle.loads(x)


def concat(lists):
    return sum(lists, [])


Python = partial(Encode, dumps, loads, concat)
