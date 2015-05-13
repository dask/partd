from partd.zmq import (create, destroy, append, get, Server, keys_to_flush, partd,
        ensure, socket, log)

from partd import core
from threading import Thread
from time import sleep
from contextlib import contextmanager

import os
import shutil

if os.path.exists('tmp.partd'):
    shutil.rmtree('tmp.partd')

def test_partd():
    with partd(available_memory=100) as (path, server):
        assert os.path.exists(path)
        assert os.path.exists(core.filename(path, '.address'))
        assert server.available_memory == 100

        append(path, {'x': b'Hello', 'y': b'abc'})
        append(path, {'x': b'World!', 'y': b'def'})

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
        s.append({'x': b'abc', 'y': b'1234'})
        assert s.memory_usage == 7
        s.append({'x': b'def', 'y': b'5678'})
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


def test_tuple_keys():
    with partd() as (path, server):
        append(path, {('x', 'y'): b'123'})
        assert get(path, [('x', 'y')]) == [b'123']


def test_flow_control():
    path = 'bar'
    if os.path.exists('bar'):
        core.destroy('bar')
    core.create('bar')
    s = Server('bar', available_memory=1, n_outstanding_writes=3, start=False)
    try:
        listen_thread = Thread(target=s.listen)
        listen_thread.start()
        """ Don't start these threads
        self._write_to_disk_thread = Thread(target=self._write_to_disk)
        self._write_to_disk_thread.start()
        self._free_frozen_sockets_thread = Thread(target=self._free_frozen_sockets)
        self._free_frozen_sockets_thread.start()
        """
        assert socket('bar')
        assert socket('bar')
        assert socket('bar')
        append('bar', {'x': '12345'})
        sleep(0.1)
        assert s._out_disk_buffer.qsize() == 1
        append('bar', {'x': '12345'})
        append('bar', {'x': '12345'})
        sleep(0.01)
        assert s._out_disk_buffer.qsize() == 3

        held_append = Thread(target=append, args=('bar', {'x': b'123'}))
        held_append.start()

        sleep(0.01)
        assert held_append.isAlive()  # held!

        assert not s._frozen_sockets.empty()

        write_to_disk_thread = Thread(target=s._write_to_disk)
        write_to_disk_thread.start()
        free_frozen_sockets_thread = Thread(target=s._free_frozen_sockets)
        free_frozen_sockets_thread.start()

        sleep(0.2)
        assert not held_append.isAlive()
        assert s._frozen_sockets.empty()
    finally:
        s.close()


from partd.zmq import PartdFile, PartdSharedServer


@contextmanager
def partd_server(path, **kwargs):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.mkdir(path)
    with Server(path, **kwargs) as server:
        with PartdSharedServer(path) as p:
            yield (p, server)


def test_partd_object():
    if os.path.exists('foo'):
        shutil.rmtree('foo')
    with partd_server('foo', available_memory=100) as (p, server):
        assert os.path.exists(p.file.path)
        assert 'ipc://server' in p.file.get('.address', lock=False)

        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']
    assert not os.path.exists(p.file.path)


def test_delete():
    with partd_server('foo', available_memory=100) as (p, server):
        p.append({'x': b'Hello'})
        assert p.get('x') == b'Hello'
        p.delete(['x'])
        assert p.get('x') == b''


def test_iset():
    with partd() as (path, server):
        ensure(path, 'x', b'111')
        ensure(path, 'x', b'111')
        assert get(path, ['x']) == [b'111']

def test_tuple_keys():
    with partd() as (path, server):
        append(path, {('x', 'y'): b'123'})
        assert get(path, [('x', 'y')]) == [b'123']
