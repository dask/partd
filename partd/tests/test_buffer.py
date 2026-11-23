from partd.dict import Dict
from partd.file import File
from partd.buffer import Buffer, keys_to_flush
import pickle

import shutil
import os


def test_partd():
    a = Dict()
    b = Dict()
    with Buffer(a, b, available_memory=10) as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        assert a.get(['x', 'y']) == [b'Hello', b'abc']

        p.append({'x': b'World!', 'y': b'def'})
        assert a.get(['x', 'y']) == [b'', b'abcdef']
        assert b.get(['x', 'y']) == [b'HelloWorld!', b'']

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)


def test_keys_to_flush():
    lengths = {'a': 20, 'b': 10, 'c': 15, 'd': 15, 'e': 10, 'f': 25, 'g': 5}
    assert keys_to_flush(lengths, 0.5) == ['f', 'a']


def test_pickle():
    with Dict() as a:
        with File() as b:
            c = Buffer(a, b)

            c.append({'x': b'123'})

            d = pickle.loads(pickle.dumps(c))

            assert d.get('x') == c.get('x')

            pickled_attrs = ('memory_usage', 'lengths', 'available_memory')
            for attr in pickled_attrs:
                assert hasattr(d, attr)
                assert getattr(d, attr) == getattr(c, attr)
            # special case Dict and File -- some attrs do not pickle
            assert hasattr(d, 'fast')
            assert d.fast.data == c.fast.data
            assert hasattr(d, 'slow')
            assert d.slow.path == c.slow.path


def test_append_order_across_spill(tmp_path):
    with Buffer(Dict(), File(str(tmp_path)), available_memory=5) as p:
        p.append({'x': b'first-'})
        p.append({'x': b'next'})
        assert p.get('x') == b'first-next'
        p.flush()
        assert p.get('x') == b'first-next'
