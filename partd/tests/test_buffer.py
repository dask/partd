from partd.dict import Dict
from partd.buffer import Buffer

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
