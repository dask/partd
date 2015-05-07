from __future__ import absolute_import

from partd.numpy import create, put, get, destroy
from partd.core import lock, filename

import numpy as np
import os
import shutil


def test_partd():
    path = 'tmp.partd'

    if os.path.exists(path):
        shutil.rmtree(path)

    dt = np.dtype([('a', 'i4'), ('b', 'i2'), ('c', 'f8')])

    assert not os.path.exists(path)
    create(path, dt)
    assert os.path.exists(path)

    put(path, {'a': np.array([10, 20, 30], dtype=dt['a']),
               'b': np.array([ 1,  2,  3], dtype=dt['b']),
               'c': np.array([.1, .2, .3], dtype=dt['c'])})
    put(path, {'a': np.array([70, 80, 90], dtype=dt['a']),
               'b': np.array([ 7,  8,  9], dtype=dt['b']),
               'c': np.array([.7, .8, .9], dtype=dt['c'])})
    assert os.path.exists(filename(path, 'a'))
    assert os.path.exists(filename(path, 'b'))
    assert os.path.exists(filename(path, 'c'))

    result = get(path, ['a', 'c'])
    assert (result[0] == np.array([10, 20, 30, 70, 80, 90],dtype=dt['a'])).all()
    assert (result[1] == np.array([.1, .2, .3, .7, .8, .9],dtype=dt['c'])).all()

    with lock(path):  # uh oh, possible deadlock
        result = get(path, ['a'], lock=False)

    destroy(path)
    assert not os.path.exists(path)
