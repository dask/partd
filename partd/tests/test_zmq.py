from partd.zmq import Server, keys_to_flush, log, File, Shared
from partd import core
from threading import Thread
from time import sleep
from contextlib import contextmanager

import os
import shutil


def test_server():
    if os.path.exists('foo'):
        core.destroy('foo')
    p = File('foo')
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
        assert p.get(['x'], lock=False) == [b'abcdef']
    finally:
        s.close()


def test_keys_to_flush():
    lengths = {'a': 20, 'b': 10, 'c': 15, 'd': 15, 'e': 10, 'f': 25, 'g': 5}
    assert keys_to_flush(lengths, 0.5) == ['f', 'a']



def test_flow_control():
    path = 'bar'
    if os.path.exists('bar'):
        shutil.rmtree('bar')
    s = Server('bar', available_memory=1, n_outstanding_writes=3, start=False)
    p = Shared('bar')
    try:
        listen_thread = Thread(target=s.listen)
        listen_thread.start()
        """ Don't start these threads
        self._write_to_disk_thread = Thread(target=self._write_to_disk)
        self._write_to_disk_thread.start()
        self._free_frozen_sockets_thread = Thread(target=self._free_frozen_sockets)
        self._free_frozen_sockets_thread.start()
        """
        p.append({'x': '12345'})
        sleep(0.1)
        assert s._out_disk_buffer.qsize() == 1
        p.append({'x': '12345'})
        p.append({'x': '12345'})
        sleep(0.1)
        assert s._out_disk_buffer.qsize() == 3

        held_append = Thread(target=p.append, args=({'x': b'123'},))
        held_append.start()

        sleep(0.1)
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




@contextmanager
def partd_server(path, **kwargs):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.mkdir(path)
    with Server(path, **kwargs) as server:
        with Shared(path) as p:
            yield (p, server)


def test_partd_object():
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
    with partd_server('foo', available_memory=100) as (p, server):
        p.iset('x', b'111')
        p.iset('x', b'111')
        assert p.get('x') == b'111'

def test_tuple_keys():
    with partd_server('foo', available_memory=100) as (p, server):
        p.append({('x', 'y'): b'123'})
        assert p.get(('x', 'y')) == b'123'
