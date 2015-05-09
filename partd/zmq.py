from __future__ import absolute_import, print_function

import zmq
from itertools import chain
from bisect import bisect
from operator import add
from time import sleep, time
from toolz import accumulate, topk, pluck
import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Thread, Lock
from toolz import keymap
from datetime import datetime
from . import core
from .compatibility import Queue, Empty

context = zmq.Context()

tuple_sep = '-|-'

def log(*args):
    with open('log', 'a') as f:
        print(datetime.now(), *args, file=f)


log('Import zmq')

@contextmanager
def logduration(message, nbytes=None):
    start = time()
    try:
        yield
    finally:
        end = time()
        log(message, end - start)
        if nbytes:
            log("MB/s:", nbytes / (end - start) / 1e6)

@contextmanager
def logerrors():
    try:
        yield
    except Exception as e:
        log('Error!', str(e))
        raise


class Server(object):
    def __init__(self, path, address=None, available_memory=1e9):
        self.path = path
        self.inmem = defaultdict(list)
        self.lengths = defaultdict(lambda: 0)
        self.socket = context.socket(zmq.ROUTER)

        if address is None:
            address = 'ipc://server-%s' % str(uuid.uuid1())
        self.address = address
        self.socket.bind(self.address)
        with open(core.filename(self.path, '.address'), 'w') as f:
            f.write(self.address)

        self.available_memory=available_memory
        self.memory_usage = 0
        self.status = 'run'
        self._out_disk_buffer = Queue(maxsize=3)
        self._frozen_sockets = Queue()

        self._file_lock = core.lock(path)
        self._file_lock.acquire()
        self._lock = Lock()
        self._socket_lock = Lock()

    def start(self):
        log('Start server at', self.address)
        self._listen_thread = Thread(target=self.listen)
        self._listen_thread.start()
        self._write_to_disk_thread = Thread(target=self._write_to_disk)
        self._write_to_disk_thread.start()
        self._free_frozen_sockets_thread = Thread(target=self._free_frozen_sockets)
        self._free_frozen_sockets_thread.start()

    def listen(self):
        with logerrors():
            log(self.address, 'start listening')
            while self.status != 'closed':
                if not self.socket.poll(100):
                    continue

                with self._socket_lock:
                    payload = self.socket.recv_multipart()

                address, command, payload = payload[0], payload[1], payload[2:]
                if command == b'close':
                    log('Server closes')
                    self.status = 'closed'
                    self.ack(address)
                    break

                elif command == b'put':
                    keys, values = payload[::2], payload[1::2]
                    keys = list(map(deserialize_key, keys))
                    data = dict(zip(keys, values))
                    self.put(data)
                    self.ack(address)

                elif command == b'get':
                    keys = list(map(deserialize_key, payload))
                    log('get', keys)
                    result = self.get(keys)
                    self.send_to_client(address, result)
                    self.ack(address, flow_control=False)

                elif command == b'syn':
                    self.ack(address)

                else:
                    log("Unknown command", command)
                    raise ValueError("Unknown command: " + command)

    def send_to_client(self, address, result):
        if not isinstance(result, list):
            result = [result]
        with self._socket_lock:
            self.socket.send_multipart([address] + result)

    def _write_to_disk(self):
        while self.status != 'closed':
            try:
                data = self._out_disk_buffer.get(timeout=1)
            except Empty:
                continue
            else:
                with self._lock:
                    nbytes = sum(map(len, data.values()))
                    with logduration("Write %d files" % len(data),
                                     nbytes=nbytes):
                        core.put(self.path, data, lock=False)
                self._out_disk_buffer.task_done()

    def ack(self, address, flow_control=True):
        if flow_control and self._out_disk_buffer.full():
            log('Out disk buffer full - Flow control in effect',
                'Freezing address', address)
            self._frozen_sockets.put(address)
        else:
            log('Server sends ack')
            self.send_to_client(address, b'ack')

    def _free_frozen_sockets(self):
        while self.status != 'closed':
            try:
                data = self._frozen_sockets.get(timeout=1)
                self._frozen_sockets.put(data)  # put back in for the moment
            except Empty:
                continue
            else:
                log('Freeing frozen sockets, waiting on disk buffer to clear')
                self._out_disk_buffer.join()
                log('Disk buffer cleared, sending acks to %d sockets' %
                        self._frozen_sockets.qsize())
                while not self._frozen_sockets.empty():
                    addr = self._frozen_sockets.get()
                    log('Free', addr)
                    self.ack(addr)

    def put(self, data):
        total_mem = 0
        for k, v in data.items():
            self.inmem[k].append(v)
            self.lengths[k] += len(v)
            total_mem += len(v)
        self.memory_usage += total_mem

        log('Server puts %d keys' % len(data.keys()), 'of %d bytes' % total_mem)

        if self.memory_usage > self.available_memory:
            keys = keys_to_flush(self.lengths, 0.25)
            self.flush(keys)

    def flush(self, keys=None, block=None):
        """ Flush keys to disk

        Parameters
        ----------

        keys: list or None
            list of keys to flush
        block: bool (defaults to None)
            Whether or not to block until all writing is complete

        If no keys are given then flush all keys
        """
        if keys is None:
            keys = list(self.lengths)
            if block is None:
                block = True
        assert isinstance(keys, (tuple, list))
        payload = dict((key, ''.join(self.inmem[key])) for key in keys)
        log('Put data into out-disk-buffer', 'nkeys', len(keys))
        self._out_disk_buffer.put(payload)

        for key in keys:
            self.memory_usage -= self.lengths[key]
            del self.inmem[key]
            del self.lengths[key]

        if block:
            log('Blocking on out disk buffer from flush')
            self._out_disk_buffer.join()

    def get(self, keys):
        log('Server gets keys', keys)
        self._out_disk_buffer.join()  # block until everything is written
        with self._lock:
            from_disk = core.get(self.path, keys, lock=False)
            result = [from_disk[i] + ''.join(self.inmem[k])
                          for i, k in enumerate(keys)]
        return result

    def close(self):
        log('Server closes')
        self.status = 'closed'
        self._file_lock.release()


def keys_to_flush(lengths, fraction=0.4):
    """ Which keys to remove

    >>> lengths = {'a': 20, 'b': 10, 'c': 15, 'd': 15,
    ...            'e': 10, 'f': 25, 'g': 5}
    >>> keys_to_flush(lengths, 0.5)
    ['f', 'a']
    """
    tophalf = topk(max(len(lengths) / 2, 1),
                       lengths.items(),
                       key=1)
    total = sum(lengths.values())
    cutoff = max(1, bisect(list(accumulate(add, pluck(1, tophalf))),
                    total * fraction))
    return [k for k, v in tophalf[:cutoff]]


def create(path, **kwargs):
    core.create(path)
    server = Server(path, **kwargs)
    server.start()
    return server


sockets = dict()

def socket(path):
    try:
        return sockets[path]
    except KeyError:
        sock = context.socket(zmq.DEALER)
        lock = Lock()
        with open(core.filename(path, '.address')) as f:
            addr = f.read()
        sock.connect(addr)
        sockets[path] = (sock, lock)
        send(path, b'syn', [], ack_required=False)
        return sock, lock


def destroy(path, server=None):
    if server:
        server.close()
    else:
        send(path, b'close', [])
    del sockets[path]
    core.destroy(path)


def get(path, keys):
    log('Client gets', path, keys)
    keys = list(map(serialize_key, keys))
    return send(path, b'get', keys, recv=True)


def put(path, data):
    log('Client puts', path, str(len(data)) + ' keys')
    data = keymap(serialize_key, data)
    payload = list(chain.from_iterable(data.items()))
    send(path, b'put', payload)


def send(path, command, payload, recv=False, ack_required=True):
    sock, lock = socket(path)
    with lock:
        if ack_required:
            ack = sock.recv_multipart()
            assert ack == [b'ack']
        log('Client sends command', command)
        sock.send_multipart([command] + payload)
        if recv:
            result = sock.recv_multipart()
        else:
            result = None
    return result

ensure = core.ensure

@contextmanager
def partd(path=None, **kwargs):
    if path is None:
        path = 'tmp.partd'
    server = create(path, **kwargs)

    try:
        yield path, server
    finally:
        destroy(path, server)


def serialize_key(key):
    """

    >>> serialize_key('x')
    'x'
    >>> serialize_key(('a', 'b', 1))
    'a-|-b-|-1'
    """
    if isinstance(key, tuple):
        return tuple_sep.join(map(serialize_key, key))
    if isinstance(key, (bytes, str)):
        return key
    return str(key)


def deserialize_key(text):
    """

    >>> deserialize_key('x')
    'x'
    >>> deserialize_key('a-|-b-|-1')
    ('a', 'b', '1')
    """
    if tuple_sep in text:
        return tuple(text.split(tuple_sep))
    else:
        return text
