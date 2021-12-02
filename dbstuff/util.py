"""Utilities
"""

from array import ArrayType
from dataclasses import dataclass, field
from mmap import mmap
import struct
from threading import Condition, Lock
from typing import Generic, Tuple, TypeVar, Union


ReadableBuffer = Union[ArrayType, bytes, bytearray, memoryview, mmap]
WriteableBuffer = Union[ArrayType, bytearray, memoryview, mmap]


K = TypeVar("K")
V = TypeVar("V")


@dataclass(order=True)
class Entry(Generic[K, V]):
    """A simple dataclass that compares on the key and ignores the value.

    >>> Entry(1, 2) == Entry(1, 3)
    True
    >>> sorted([Entry(2, 1000), Entry(1, {}), Entry (3, None)])
    [Entry(key=1, value={}), Entry(key=2, value=1000), Entry(key=3, value=None)]
    """

    key: K = field(hash=True)
    value: V = field(compare=False, hash=False)


def split_list(x: list) -> Tuple[list, list]:
    """Split a list.

    For an odd-length list, the median goes to the right.

    :param x: the list to split
    :type x: list
    :return: A tuple containing each half of the list
    :rtype: Tuple[list]

    >>> split_list([1, 2, 3, 4])
    ([1, 2], [3, 4])
    >>> split_list([1, 2, 3, 4, 5])
    ([1, 2], [3, 4, 5])
    """

    median = len(x) // 2
    return x[:median], x[median:]


def length_prefix(data: ReadableBuffer) -> ReadableBuffer:
    """Prefix binary data with a 32 bit length counter."""
    size = len(data)
    return struct.pack(f"!I{size}s", size, data)


class ReadWriteLock:
    def __init__(self):
        self._read_lock = Lock()
        self._write_lock = Condition(Lock())
        self._reader_count = 0
        self.read_access = _ReaderContext(self)
        self.write_access = _WriterContext(self)

    def acquire_read(self):
        with self._read_lock:
            with self._write_lock:
                self._reader_count += 1

    def release_read(self):
        with self._write_lock:
            self._reader_count -= 1
            if not self._reader_count:
                self._write_lock.notify()

    def acquire_write(self):
        self._read_lock.acquire()
        self._write_lock.acquire()
        self._write_lock.wait_for(lambda: self._reader_count == 0)

    def release_write(self):
        self._write_lock.notify()
        self._write_lock.release()
        self._read_lock.release()


class _ReaderContext:
    def __init__(self, rwlock: ReadWriteLock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_read()

    def __exit__(self, _type, _value, _tb):
        self.rwlock.release_read()


class _WriterContext:
    def __init__(self, rwlock: ReadWriteLock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_write()

    def __exit__(self, _type, _value, _tb):
        self.rwlock.release_write()
