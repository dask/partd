from partd.zmq import create, destroy, put, get, Server, keys_to_flush

from partd import core

import os
import shutil

def test_partd():
    path = 'tmp.partd'

    if os.path.exists(path):
        shutil.rmtree(path)

    assert not os.path.exists(path)
    server = create(path, available_memory=100)
    assert os.path.exists(path)
    assert os.path.exists(core.filename(path, '.address'))
    assert server.available_memory == 100

    put(path, {'x': b'Hello', 'y': b'abc'})
    put(path, {'x': b'World!', 'y': b'def'})

    result = get(path, ['y', 'x'])
    assert result == [b'abcdef', b'HelloWorld!']

    destroy(path)
    assert not os.path.exists(path)


def test_server():
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
    finally:
        s.close()


def test_keys_to_flush():
    lengths = {'a': 20, 'b': 10, 'c': 15, 'd': 15, 'e': 10, 'f': 25, 'g': 5}
    assert keys_to_flush(lengths, 0.5) == ['f', 'a']
