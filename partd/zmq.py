from __future__ import absolute_import, print_function

import zmq
from itertools import chain
import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Thread
from . import core

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
        self.lock = core.lock(path)
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

    def start(self):
        self._listen_thread = Thread(target=self.listen).start()

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

    def put(self, data):
        for k, v in data.items():
            self.inmem[k].append(v)
            self.lengths[k] += len(v)
            self.memory_usage += len(v)

        if self.memory_usage > self.available_memory:
            self.flush_some()

    def flush_some(self):
        avg = mean(self.lengths.values())
        keys = [k for k, v in self.lengths.items() if v > avg]
        self.flush_keys(keys)

    def flush_keys(self, keys):
        assert isinstance(keys, (tuple, list))
        core.put(self.path, dict((key, ''.join(self.inmem[key])) for key in keys))
        for key in keys:
            self.memory_usage -= self.lengths[key]
            del self.inmem[key]
            del self.lengths[key]

    def get(self, keys):
        from_disk = core.get(self.path, keys)
        result = [from_disk[i] + ''.join(self.inmem[k])
                      for i, k in enumerate(keys)]
        return result

    def close(self):
        self.status = 'closed'


def create(path, **kwargs):
    core.create(path)
    server = Server(path)
    server.start()



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


def mean(seq):
    seq = list(seq)
    return sum(seq) / float(len(seq))
