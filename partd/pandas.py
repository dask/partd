from __future__ import absolute_import

import pandas as pd
from toolz import valmap
from functools import partial
from .compatibility import pickle
from .numpy import Numpy
from .core import Interface
from .utils import extend


dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)


class PandasColumns(Interface):
    def __init__(self, partd=None):
        self.partd = Numpy(partd)
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



import pandas as pd
from pandas.core.internals import create_block_manager_from_blocks, make_block
from pandas.core.index import _ensure_index


def to_blocks(df):
    blocks = [block.values for block in df._data.blocks]
    index = df.index.values
    placement = [ b.mgr_locs.as_array for b in df._data.blocks ]
    return blocks, index, df.index.name, list(df.columns), placement
    return {'blocks': blocks,
            'index': index,
            'index_name': df.index.name,
            'columns': df.columns,
            'placement': [ b.mgr_locs.as_array for b in df._data.blocks ]}


def from_blocks(blocks, index, index_name, columns, placement):
    blocks = [ make_block(b, placement=placement[i]) for i, b in enumerate(blocks) ]
    axes = [_ensure_index(columns), _ensure_index(index) ]
    df = pd.DataFrame(create_block_manager_from_blocks(blocks, axes))
    df.index.name = index_name
    return df

from . import numpy as pnp
from .utils import framesplit, frame

def serialize(df):
    """ Serialize and compress a Pandas DataFrame

    Uses Pandas blocks, snappy, and blosc to deconstruct an array into bytes
    """
    blocks, index, index_name, columns, placement = to_blocks(df)
    categories = [(b.ordered, b.categories)
                  if isinstance(b, pd.Categorical)
                  else None
                  for b in blocks]
    blocks = [b.codes if isinstance(b, pd.Categorical) else b
              for b in blocks]
    b_blocks = [pnp.compress(pnp.serialize(block), block.dtype)
                for block in blocks]  # this can be slightly faster if we merge both operations
    b_index = pnp.compress(pnp.serialize(index), index.dtype)
    frames = [dumps(index_name),
              dumps(columns),
              dumps(placement),
              dumps(index.dtype),
              b_index,
              dumps([block.dtype for block in blocks]),
              dumps([block.shape for block in blocks]),
              dumps(categories)] + b_blocks

    return b''.join(map(frame, frames))


def deserialize(bytes):
    """ Deserialize and decompress bytes back to a pandas DataFrame """
    frames = list(framesplit(bytes))
    index_name = pickle.loads(frames[0])
    columns = pickle.loads(frames[1])
    placement = pickle.loads(frames[2])
    dt = pickle.loads(frames[3])
    index = pnp.deserialize(pnp.decompress(frames[4], dt), dt, copy=True)
    dtypes = pickle.loads(frames[5])
    shapes = pickle.loads(frames[6])
    categories = pickle.loads(frames[7])
    b_blocks = frames[8:]
    blocks = [pnp.deserialize(pnp.decompress(block, dt), dt, copy=True).reshape(shape)
                for block, dt, shape in zip(b_blocks, dtypes, shapes)]
    blocks = [pd.Categorical.from_codes(b, cat[1], ordered=cat[0])
              if cat is not None
              else b
              for cat, b in zip(categories, blocks)]

    return from_blocks(blocks, index, index_name, columns, placement)


from .encode import Encode


def join(dfs):
    if not dfs:
        return pd.DataFrame()
    else:
        return pd.concat(dfs)

PandasBlocks = partial(Encode, serialize, deserialize, join)
