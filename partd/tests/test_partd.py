from partd import create, put, get, destroy, ensure, partd
from partd.core import lock, token, escape_filename, filename
from partd import core
import os
import shutil
from contextlib import contextmanager


def test_part2():
    path = 'tmp.partd'

    with partd(path):
        put(path, {'x': b'Hello', 'y': b'abc'})
        put(path, {'x': b'World!', 'y': b'def'})
        assert os.path.exists(filename(path, 'x'))
        assert os.path.exists(filename(path, 'y'))

        result = get(path, ['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert get(path, ['z']) == [b'']

        with lock(path):  # uh oh, possible deadlock
            result = get(path, ['x'], lock=False)

    assert not os.path.exists(path)


def test_key_tuple():
    with partd() as pth:
        put(pth, {('a', 'b'): b'123'})
        assert os.path.exists(os.path.join(pth, 'a', 'b'))


def test_ensure():
    with partd() as pth:
        ensure(pth, 'x', b'123')
        ensure(pth, 'x', b'123')
        ensure(pth, 'x', b'123')

        assert get(pth, ['x']) == [b'123']

    assert (pth, 'x') not in core._ensured


def test_filenames():
    assert token('hello') == 'hello'
    assert token(('hello', 'world')) == os.path.join('hello', 'world')
    assert escape_filename(os.path.join('a', 'b')) == os.path.join('a', 'b')
    assert filename('dir', ('a', 'b')) == os.path.join('dir', 'a', 'b')
