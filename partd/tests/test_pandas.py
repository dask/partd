from __future__ import absolute_import

import pytest
pytest.importorskip('numpy')

import pandas as pd
import pandas.util.testing as tm
import os
import shutil

from partd.pandas import PandasColumns, PandasBlocks, serialize, deserialize


df1 = pd.DataFrame({'a': [1, 2, 3],
                    'b': [1., 2., 3.],
                    'c': ['x', 'y', 'x']}, columns=['a', 'b', 'c'],
                    index=pd.Index([1, 2, 3], name='myindex'))

df2 = pd.DataFrame({'a': [10, 20, 30],
                    'b': [10., 20., 30.],
                    'c': ['X', 'Y', 'X']}, columns=['a', 'b', 'c'],
                    index=pd.Index([10, 20, 30], name='myindex'))


def test_PandasColumns():
    with PandasColumns() as p:
        assert os.path.exists(p.partd.partd.path)

        p.append({'x': df1, 'y': df2})
        p.append({'x': df2, 'y': df1})
        assert os.path.exists(p.partd.partd.filename('x'))
        assert os.path.exists(p.partd.partd.filename(('x', 'a')))
        assert os.path.exists(p.partd.partd.filename(('x', '.index')))
        assert os.path.exists(p.partd.partd.filename('y'))

        result = p.get(['y', 'x'])
        tm.assert_frame_equal(result[0], pd.concat([df2, df1]))
        tm.assert_frame_equal(result[1], pd.concat([df1, df2]))

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(p.partd.partd.path)



def test_column_selection():
    with PandasColumns('foo') as p:
        p.append({'x': df1, 'y': df2})
        p.append({'x': df2, 'y': df1})
        result = p.get('x', columns=['c', 'b'])
        tm.assert_frame_equal(result, pd.concat([df1, df2])[['c', 'b']])


def test_PandasBlocks():
    with PandasBlocks() as p:
        assert os.path.exists(p.partd.path)

        p.append({'x': df1, 'y': df2})
        p.append({'x': df2, 'y': df1})
        assert os.path.exists(p.partd.filename('x'))
        assert os.path.exists(p.partd.filename('y'))

        result = p.get(['y', 'x'])
        tm.assert_frame_equal(result[0], pd.concat([df2, df1]))
        tm.assert_frame_equal(result[1], pd.concat([df1, df2]))

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(p.partd.path)


@pytest.mark.parametrize('ordered', [False, True])
def test_serialize_categoricals(ordered):
    df = pd.DataFrame({'x': [1, 2, 3, 4],
                       'y': pd.Categorical(['c', 'a', 'b', 'a'],
                                           ordered=ordered)})

    df2 = deserialize(serialize(df))
    tm.assert_frame_equal(df, df2)
    assert df.y.cat.ordered == df2.y.cat.ordered
