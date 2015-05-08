from __future__ import absolute_import, print_function

import zmq
from itertools import chain
from bisect import bisect
from operator import add
from time import sleep
from toolz import accumulate, topk, pluck
import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Thread, Lock
from . import core
from .compatibility import Queue, Empty

context = zmq.Context()

with open('log', 'w') as f:  # delete file
    pass

def log(*args):
    with open('log', 'a') as f:
        print(*args, file=f)


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

        self._file_lock = core.lock(path)
        self._file_lock.acquire()
        self._lock = Lock()

    def start(self):
        self._listen_thread = Thread(target=self.listen)
        self._listen_thread.start()
        self._write_to_disk_thread = Thread(target=self._write_to_disk)
        self._write_to_disk_thread.start()

    def listen(self):
        while self.status != 'closed':
            if not self.socket.poll(100):
                continue

            payload = self.socket.recv_multipart()

            address, command, payload = payload[0], payload[1], payload[2:]
            if command == b'close':
                self.status = 'closed'
                break

            if command == b'put':
                data = dict(zip(payload[::2], payload[1::2]))
                self.put(data)

            if command == b'get':
                result = self.get(payload)
                self.socket.send_multipart([address] + result)

    def _write_to_disk(self):
        while self.status != 'closed':
            try:
                data = self._out_disk_buffer.get(timeout=0.1)
            except Empty:
                continue
            else:
                with self._lock:
                    core.put(self.path, data, lock=False)
                self._out_disk_buffer.task_done()

    def put(self, data):
        with self._lock:
            for k, v in data.items():
                self.inmem[k].append(v)
                self.lengths[k] += len(v)
                self.memory_usage += len(v)

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
        self._out_disk_buffer.put(payload)

        for key in keys:
            self.memory_usage -= self.lengths[key]
            del self.inmem[key]
            del self.lengths[key]

        if block:
            self._out_disk_buffer.join()

    def get(self, keys):
        self._out_disk_buffer.join()  # block until everything is written
        with self._lock:
            from_disk = core.get(self.path, keys, lock=False)
            result = [from_disk[i] + ''.join(self.inmem[k])
                          for i, k in enumerate(keys)]
        return result

    def close(self):
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
        with open(core.filename(path, '.address')) as f:
            addr = f.read()
        sock.connect(addr)
        sockets[path] = sock
        return sock


def destroy(path):
    sock = socket(path)
    sock.send_multipart(['close'])
    core.destroy(path)


def get(path, keys):
    sock = socket(path)
    sock.send_multipart([b'get'] + keys)
    return sock.recv_multipart()


def put(path, data):
    sock = socket(path)
    payload = list(chain.from_iterable(data.items()))
    sock.send_multipart([b'put'] + payload)