from partd import create, put, get, destroy, filename
from partd.core import lock
import os
import shutil


def test_part2():
    path = 'tmp.partd'

    if os.path.exists(path):
        shutil.rmtree(path)

    assert not os.path.exists(path)
    create(path)
    assert os.path.exists(path)

    put(path, {'x': b'Hello', 'y': b'abc'})
    put(path, {'x': b'World!', 'y': b'def'})
    assert os.path.exists(filename(path, 'x'))
    assert os.path.exists(filename(path, 'y'))

    result = get(path, ['y', 'x'])
    assert result == [b'abcdef', b'HelloWorld!']

    assert get(path, ['z']) == [b'']

    with lock(path):  # uh oh, possible deadlock
        result = get(path, ['x'], lock=False)

    destroy(path)
    assert not os.path.exists(path)
