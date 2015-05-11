from contextlib import contextmanager
import os
import shutil
import tempfile
import struct


def raises(exc, lamda):
    try:
        lamda()
        return False
    except exc:
        return True


@contextmanager
def tmpfile(extension=''):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension)
    os.close(handle)
    os.remove(filename)

    try:
        yield filename
    finally:
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                os.remove(filename)


def frame(bytes):
    return struct.pack('Q', len(bytes)) + bytes


def framesplit(bytes):
    i = 0; n = len(bytes)
    chunks = list()
    while i < n:
        nbytes = struct.unpack('Q', bytes[i:i+8])[0]
        i += 8
        yield bytes[i: i + nbytes]
        i += nbytes
