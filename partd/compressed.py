from .utils import ignoring
from .encode import Encode
from functools import partial

__all__ = []

with ignoring(ImportError):
    import snappy
    Snappy = partial(Encode,
                     snappy.compress,
                     snappy.decompress,
                     b''.join)
    __all__.append('Snappy')


with ignoring(ImportError):
    import zlib
    ZLib = partial(Encode,
                   zlib.compress,
                   zlib.decompress,
                   b''.join)
    __all__.append('ZLib')


with ignoring(ImportError):
    import bz2
    BZ2 = partial(Encode,
                  bz2.compress,
                  bz2.decompress,
                  b''.join)
    __all__.append('BZ2')


with ignoring(ImportError):
    import blosc
    Blosc = partial(Encode,
                    blosc.compress,
                    blosc.decompress,
                    b''.join)
    __all__.append('Blosc')
