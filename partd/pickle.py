"""
get/put functions that consume/produce Python lists using Pickle to serialize
"""
from __future__ import absolute_import
from .compatibility import pickle


from .encode import PartdEncode
from functools import partial

def concat(lists):
    return sum(lists, [])

PartdPickle = partial(PartdEncode,
                      partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL),
                      pickle.loads,
                      join=concat)
