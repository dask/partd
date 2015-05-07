"""
get/put functions that consume/produce Python lists using Pickle to serialize
"""

from __future__ import absolute_import
from . import core
from .compatibility import pickle
from .utils import tmpfile


create = core.create
destroy = core.destroy


def multi_loads(data):
    """ Load a sequence of pickled lists stored in one string

    >>> data = pickle.dumps([1, 2, 3]) + pickle.dumps([4, 5, 6])
    >>> multi_loads(data)
    [1, 2, 3, 4, 5, 6]
    """
    # f = StringIO(data)  # This performs really slowly for some reason
    # Instead we create a file on disk :-(
    result = []
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(data)
        with open(fn) as f:
            while True:
                try:
                    result.extend(pickle.load(f))
                except EOFError:
                    return result


def put(path, data, protocol=pickle.HIGHEST_PROTOCOL, put=core.put, **kwargs):
    """ Put dict of Python lists into store """
    data = dict((k, pickle.dumps(v, protocol=protocol))
                for k, v in data.items())
    put(path, data, **kwargs)


def get(path, keys, get=core.get, **kwargs):
    """ Retrieve Python lists from store """
    return list(map(multi_loads, get(path, keys, **kwargs)))
