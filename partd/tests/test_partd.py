from partd import create, put, get, destroy, filename
from partd.core import lock, token, escape_filename
import os
import shutil
from contextlib import contextmanager


def test_part2():
    path = 'tmp.partd'

    with part(path):
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


@contextmanager
def part(path='tmp.partd'):
    if os.path.exists(path):
        shutil.rmtree(path)

    create(path)

    try:
        yield path
    finally:
        destroy(path)


def test_key_tuple():
    with part() as pth:
        put(pth, {('a', 'b'): b'123'})
        assert os.path.exists(os.path.join(pth, 'a', 'b'))


def test_filenames():
    assert token('hello') == 'hello'
    assert token(('hello', 'world')) == os.path.join('hello', 'world')
    assert escape_filename(os.path.join('a', 'b')) == os.path.join('a', 'b')
    assert filename('dir', ('a', 'b')) == os.path.join('dir', 'a', 'b')
