from .core import PartdInterface
from .file import PartdFile
from toolz import valmap
from .utils import frame, framesplit


class PartdEncode(PartdInterface):
    def __init__(self, encode, decode, partd, join=''.join):
        if isinstance(partd, str):
            partd = PartdFile(partd)
        self.partd = partd
        self.encode = encode
        self.decode = decode
        self.join = join
        PartdInterface.__init__(self)

    def append(self, data, **kwargs):
        data = valmap(self.encode, data)
        data = valmap(frame, data)
        self.partd.append(data, **kwargs)

    def _get(self, keys, **kwargs):
        raw = self.partd._get(keys, **kwargs)
        return [self.join([self.decode(frame) for frame in framesplit(chunk)])
                for chunk in raw]

    def delete(self, keys, **kwargs):
        return self.partd.delete(keys, **kwargs)

    def _iset(self, key, value, **kwargs):
        with self.partd.lock:
            if self.partd.get(key, lock=False):
                self.delete(key, lock=False, **kwargs)
            self.append({key: value}, lock=False, **kwargs)

    def drop(self):
        pass # return self.partd.drop()

    @property
    def lock(self):
        return self.partd.lock

    def __exit__(self, *args):
        self.partd.__exit__(*args)
        PartdInterface.__exit__(self, *args)
