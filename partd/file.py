from __future__ import absolute_import

from .core import Interface
import locket
import os
import shutil
import string


class File(Interface):
    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.lock = locket.lock_file(self.filename('.lock'))
        Interface.__init__(self)

    def __getstate__(self):
        return {'path': self.path}

    def __setstate__(self, state):
        Interface.__setstate__(self, state)
        self.lock = locket.lock_file(self.filename('.lock'))

    def append(self, data, lock=True, **kwargs):
        if lock: self.lock.acquire()
        try:
            for k, v in data.items():
                fn = self.filename(k)
                if not os.path.exists(os.path.dirname(fn)):
                    os.makedirs(os.path.dirname(fn))
                with open(fn, 'ab') as f:
                    f.write(v)
                    os.fsync(f)
        finally:
            if lock: self.lock.release()

    def _get(self, keys, lock=True, **kwargs):
        assert isinstance(keys, (list, tuple, set))
        if lock:
            self.lock.acquire()
        try:
            result = []
            for key in keys:
                try:
                    with open(self.filename(key), 'rb') as f:
                        result.append(f.read())
                except IOError:
                    result.append(b'')
        finally:
            if lock:
                self.lock.release()
        return result

    def _iset(self, key, value):
        """ Idempotent set """
        fn = self.filename(key)
        if not os.path.exists(os.path.dirname(fn)):
            os.makedirs(os.path.dirname(fn))
        with open(self.filename(key), 'w') as f:
            f.write(value)

    def _delete(self, keys, lock=True):
        if lock:
            self.lock.acquire()
        try:
            for key in keys:
                os.remove(filename(self.path, key))
        finally:
            if lock:
                self.lock.release()

    def drop(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def filename(self, key):
        return filename(self.path, key)


def filename(path, key):
    return os.path.join(path, escape_filename(token(key)))


# http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
valid_chars = "-_.() " + string.ascii_letters + string.digits + os.path.sep


def escape_filename(fn):
    """ Escape text so that it is a valid filename

    >>> escape_filename('Foo!bar?')
    'Foobar'

    """
    return ''.join(filter(valid_chars.__contains__, fn))



def token(key):
    """

    >>> token('hello')
    'hello'
    >>> token(('hello', 'world'))  # doctest: +SKIP
    'hello/world'
    """
    if isinstance(key, str):
        return key
    elif isinstance(key, tuple):
        return os.path.join(*map(token, key))
    else:
        return str(key)
