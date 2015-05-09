from partd.pickle import get, put, create, destroy, partd, ensure
from partd.core import filename, lock


import os
import shutil

def test_pickle():
    with partd() as path:
        put(path, {'x': ['Hello', 'World!'], 'y': [1, 2, 3]})
        put(path, {'x': ['Alice', 'Bob!'], 'y': [4, 5, 6]})
        assert os.path.exists(filename(path, 'x'))
        assert os.path.exists(filename(path, 'y'))

        result = get(path, ['y', 'x'])
        assert result == [[1, 2, 3, 4, 5, 6],
                          ['Hello', 'World!', 'Alice', 'Bob!']]

        with lock(path):  # uh oh, possible deadlock
            result = get(path, ['x'], lock=False)

    assert not os.path.exists(path)


def test_ensure():
    with partd() as path:
        ensure(path, 'x', [1, 2, 3])
        ensure(path, 'x', [1, 2, 3])

        assert get(path, ['x']) == [[1, 2, 3]]
