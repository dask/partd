from partd.zmq import (create, destroy, put, get, Server, keys_to_flush, partd,
        ensure)

from partd import core

import os
import shutil

def test_partd():
    with partd(available_memory=100) as (path, server):
        assert os.path.exists(path)
        assert os.path.exists(core.filename(path, '.address'))
        assert server.available_memory == 100

        put(path, {'x': b'Hello', 'y': b'abc'})
        put(path, {'x': b'World!', 'y': b'def'})

        result = get(path, ['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']
    assert not os.path.exists(path)


def test_server():
    if os.path.exists('foo'):
        core.destroy('foo')
    core.create('foo')
    s = Server('foo', available_memory=10)
    try:
        s.start()
        s.put({'x': b'abc', 'y': b'1234'})
        assert s.memory_usage == 7
        s.put({'x': b'def', 'y': b'5678'})
        assert s.memory_usage < s.available_memory

        assert s.get(['x']) == [b'abcdef']
        assert s.get(['x', 'y']) == [b'abcdef', b'12345678']

        s.flush(block=True)

        assert s.memory_usage == 0
        assert core.get('foo', ['x'], lock=False) == [b'abcdef']
    finally:
        s.close()


def test_ensure():
    with partd() as (path, server):
        ensure(path, 'x', b'111')
        ensure(path, 'x', b'111')
        assert get(path, ['x']) == [b'111']

def test_keys_to_flush():
    lengths = {'a': 20, 'b': 10, 'c': 15, 'd': 15, 'e': 10, 'f': 25, 'g': 5}
    assert keys_to_flush(lengths, 0.5) == ['f', 'a']
