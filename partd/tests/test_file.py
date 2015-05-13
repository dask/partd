from partd.file import PartdFile

import shutil
import os


def test_partd():
    if os.path.exists('foo'):
        shutil.rmtree('foo')

    with PartdFile('foo') as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})
        assert os.path.exists(p.filename('x'))
        assert os.path.exists(p.filename('y'))

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(p.path)


def test_key_tuple():
    with PartdFile('foo') as p:
        p.append({('a', 'b'): b'123'})
        assert os.path.exists(p.filename(('a', 'b')))


def test_ensure():
    with PartdFile('foo') as p:
        p.iset('x', b'123')
        p.iset('x', b'123')
        p.iset('x', b'123')
        assert p.get('x') == b'123'
