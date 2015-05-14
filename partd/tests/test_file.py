from partd.file import File

import shutil
import os


def test_partd():
    if os.path.exists('foo'):
        shutil.rmtree('foo')

    with File('foo') as p:
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
    with File('foo') as p:
        p.append({('a', 'b'): b'123'})
        assert os.path.exists(p.filename(('a', 'b')))


def test_iset():
    with File('foo') as p:
        p.iset('x', b'123')
        assert 'x' in p._iset_seen
        assert 'y' not in p._iset_seen
        p.iset('x', b'123')
        p.iset('x', b'123')
        assert p.get('x') == b'123'


def test_nested_get():
    with File('foo') as p:
        p.append({'x': b'1', 'y': b'2', 'z': b'3'})
        assert p.get(['x', ['y', 'z']]) == [b'1', [b'2', b'3']]


def test_drop():
    with File('foo') as p:
        p.append({'x': b'123'})
        p.iset('y', b'abc')
        assert p.get('x') == b'123'
        assert p.get('y') == b'abc'

        p.drop()
        assert p.get('x') == b''
        assert p.get('y') == b''

        p.append({'x': b'123'})
        p.iset('y', b'def')
        assert p.get('x') == b'123'
        assert p.get('y') == b'def'
