from __future__ import absolute_import

from functools import partial

import pandas as pd
from pandas.core.internals import create_block_manager_from_blocks, make_block

from . import numpy as pnp
from .core import Interface
from .compatibility import pickle
from .encode import Encode
from .utils import extend, framesplit, frame


dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)


class PandasColumns(Interface):
    def __init__(self, partd=None):
        self.partd = pnp.Numpy(partd)
        Interface.__init__(self)

    def append(self, data, **kwargs):
        for k, df in data.items():
            self.iset(extend(k, '.columns'), dumps(list(df.columns)))
            self.iset(extend(k, '.index-name'), dumps(df.index.name))

        # TODO: don't use values, it does some work.  Look at _blocks instead
        #       pframe/cframe do this well
        arrays = dict((extend(k, col), df[col].values)
                       for k, df in data.items()
                       for col in df.columns)
        arrays.update(dict((extend(k, '.index'), df.index.values)
                            for k, df in data.items()))
        # TODO: handle categoricals
        self.partd.append(arrays, **kwargs)

    def _get(self, keys, columns=None, **kwargs):
        if columns is None:
            columns = self.partd.partd.get([extend(k, '.columns') for k in keys],
                                           **kwargs)
            columns = list(map(pickle.loads, columns))
        else:
            columns = [columns] * len(keys)
        index_names = self.partd.partd.get([extend(k, '.index-name')
                                            for k in keys], **kwargs)
        index_names = map(pickle.loads, index_names)

        keys = [[extend(k, '.index'), [extend(k, col) for col in cols]]
                 for k, cols in zip(keys, columns)]

        arrays = self.partd.get(keys, **kwargs)

        return [pd.DataFrame(dict(zip(cols, arrs)), columns=cols,
                             index=pd.Index(index, name=iname))
            for iname, (index, arrs), cols in zip(index_names, arrays, columns)]

    def __getstate__(self):
        return {'partd': self.partd}

    def _iset(self, key, value):
        return self.partd._iset(key, value)

    def drop(self):
        return self.partd.drop()

    @property
    def lock(self):
        return self.partd.partd.lock

    def __exit__(self, *args):
        self.drop()
        self.partd.__exit__(self, *args)

    def __del__(self):
        self.partd.__del__()


def to_names_values_placement(df):
    names = (df.columns.name, df.index.name)
    values = [df.columns.values, df.index.values]
    values.extend([block.values for block in df._data.blocks])
    placement = [b.mgr_locs.as_array for b in df._data.blocks]
    return names, values, placement


def from_names_values_placement(names, values, placement):
    axes = [pd.Index(values[0], name=names[0]),
            pd.Index(values[1], name=names[1])]
    blocks = [make_block(b, placement=placement[i])
              for i, b in enumerate(values[2:])]
    return pd.DataFrame(create_block_manager_from_blocks(blocks, axes))


def serialize(df):
    """ Serialize and compress a Pandas DataFrame

    Uses Pandas blocks, snappy, and blosc to deconstruct an array into bytes
    """
    names, values, placement = to_names_values_placement(df)

    categories = [(x.ordered, x.categories) if isinstance(x, pd.Categorical)
                   else None for x in values]
    values = [x.codes if isinstance(x, pd.Categorical) else x
              for x in values]
    # this can be slightly faster if we merge both operations
    b_values = [pnp.compress(pnp.serialize(x), x.dtype) for x in values]

    frames = [dumps(names),
              dumps(placement),
              dumps([x.dtype for x in values]),
              dumps([x.shape for x in values]),
              dumps(categories)] + b_values

    return b''.join(map(frame, frames))


def deserialize(bytes):
    """ Deserialize and decompress bytes back to a pandas DataFrame """
    frames = list(framesplit(bytes))
    names = pickle.loads(frames[0])
    placement = pickle.loads(frames[1])
    dtypes = pickle.loads(frames[2])
    shapes = pickle.loads(frames[3])
    categories = pickle.loads(frames[4])
    b_values = frames[5:]
    values = [pnp.deserialize(pnp.decompress(block, dt),
                              dt, copy=True).reshape(shape)
              for block, dt, shape in zip(b_values, dtypes, shapes)]
    values = [pd.Categorical.from_codes(b, cat[1], ordered=cat[0])
              if cat is not None else b
              for cat, b in zip(categories, values)]

    return from_names_values_placement(names, values, placement)


def join(dfs):
    if not dfs:
        return pd.DataFrame()
    else:
        return pd.concat(dfs)

PandasBlocks = partial(Encode, serialize, deserialize, join)
