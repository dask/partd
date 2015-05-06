"""
get/put functions that consume/produce Python lists using Pickle to serialize
"""

from __future__ import absolute_import
from . import core
from .compatibility import StringIO, pickle


def multi_loads(data):
    """ Load a sequence of pickled lists stored in one string

    >>> data = pickle.dumps([1, 2, 3]) + pickle.dumps([4, 5, 6])
    >>> multi_loads(data)
    [1, 2, 3, 4, 5, 6]
    """
    s = StringIO(data)
    result = []
    while True:
        try:
            result.extend(pickle.load(s))
        except EOFError:
            return result


def put(path, data, protocol=pickle.HIGHEST_PROTOCOL, put=core.put):
    """ Put dict of Python lists into store """
    data = dict((k, pickle.dumps(v, protocol=protocol))
                for k, v in data.items())
    put(path, data)


def get(path, keys, get=core.get):
    """ Retrieve Python lists from store """
    return list(map(multi_loads, get(path, keys)))

