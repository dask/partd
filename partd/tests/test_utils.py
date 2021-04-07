import struct

import pytest

from partd.utils import frame, framesplit, safer_eval


def test_frame():
    assert frame(b'Hello') == struct.pack('Q', 5) + b'Hello'


def test_framesplit():
    L = [b'Hello', b'World!', b'123']
    assert list(framesplit(b''.join(map(frame, L)))) == L


def test_safer_eval_safe():
    assert safer_eval("[1, 2, 3]") == [1, 2, 3]
    assert safer_eval("['a', 'b', 'c']") == ['a', 'b', 'c']


def test_safer_eval_unsafe():
    with pytest.raises(ValueError) as excinfo:
        safer_eval("\xe1")
    assert "non-printable" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        safer_eval("__import__('os').system('ls')")
    assert "__" in str(excinfo.value)
