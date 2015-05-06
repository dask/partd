from partd.pickle import get, put, create, destroy
from partd import filename


import os
import shutil

def test_part2():
    path = 'tmp.partd'

    if os.path.exists(path):
        shutil.rmtree(path)

    assert not os.path.exists(path)
    create(path)
    assert os.path.exists(path)

    put(path, {'x': ['Hello', 'World!'], 'y': [1, 2, 3]})
    put(path, {'x': ['Alice', 'Bob!'], 'y': [4, 5, 6]})
    assert os.path.exists(filename(path, 'x'))
    assert os.path.exists(filename(path, 'y'))

    result = get(path, ['y', 'x'])
    assert result == [[1, 2, 3, 4, 5, 6],
                      ['Hello', 'World!', 'Alice', 'Bob!']]

    destroy(path)
    assert not os.path.exists(path)

