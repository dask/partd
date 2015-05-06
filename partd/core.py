from __future__ import absolute_import

import os
import shutil
import locket
from toolz import memoize


locks = dict()


def lock(path):
    try:
        return locks[path]
    except KeyError:
        lock = locket.lock_file(os.path.join(path, '.lock'))
        locks[path] = lock
    return lock


def filename(path, key):
    return os.path.join(path, str(key))


def create(path):
    os.mkdir(path)
    lock(path)


def put(path, data):
    with lock(path):
        for k, v in data.items():
            with open(filename(path, k), 'ab') as f:
                f.write(v)


def get(path, keys):
    with lock(path):
        result = []
        for key in keys:
            with open(filename(path, key), 'rb') as f:
                result.append(f.read())
    return result


def destroy(path):
    shutil.rmtree(path)
