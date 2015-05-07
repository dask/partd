from partd.zmq import create, destroy, put, get, Server

from partd import core

import os
import shutil

def test_partd():
    path = 'tmp.partd'

    if os.path.exists(path):
        shutil.rmtree(path)

    assert not os.path.exists(path)
    create(path)
    assert os.path.exists(path)
    assert os.path.exists(core.filename(path, '.address'))

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
    s.put({'x': b'abc', 'y': b'1234'})
    assert s.memory_usage == 7
    s.put({'x': b'def', 'y': b'5678'})
    assert s.memory_usage < s.available_memory

    assert s.get(['x']) == [b'abcdef']
    assert s.get(['x', 'y']) == [b'abcdef', b'12345678']
