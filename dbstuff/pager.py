"""Abstract all file reads and writes. Manage free space.
    Zero Page Format:
        u8[8]       MAGIC
        u32         next free pageno
        u32         next overflow pageno
        u32         current overflow pageno (initially 0)
        u16         current overflow offset (initially 0)
        u8[4074]    overflow data

    Free Page Format:
        u8[8]       MAGIC
        u32         next free pageno
        u8[4080]    padding

    Overflow Page Format:
        u8[8]       MAGIC
        u32         next overflow pageno
        u8[4084]    overflow data
"""

from abc import ABC, abstractmethod
from dbstuff.util import length_prefix, ReadableBuffer, WriteableBuffer
import os
from struct import Struct, unpack_from
from typing import AnyStr, BinaryIO, Optional, Tuple, Union

from dbstuff.cache import LRUCache


# DREAM: In some future faraway dream world this is configurable instead of hard coded.
PAGESIZE = 0x1000


class Page(ABC):
    STRUCT: Struct
    MAGIC: bytes

    def __init_subclass__(cls) -> None:
        # Verify that the page type is the correct size.
        assert cls.STRUCT.size == PAGESIZE
        print(cls.__name__, "is go!")

    @classmethod
    def get_fields(cls, data: ReadableBuffer):
        if len(data) != PAGESIZE:
            raise ValueError(
                f"{cls.__name__} data has incorrect length: "
                f"{len(data)} instead of {PAGESIZE}"
            )

        fields = cls.STRUCT.unpack(data)
        if fields[0] != cls.MAGIC:
            raise ValueError(f"{cls.__name__} has incorrect magic number")
        return fields

    @classmethod
    def from_page(cls, data: ReadableBuffer):
        # as long as we define our arguments in the correct order, this will work.
        fields = cls.get_fields(data)
        return cls(*fields[1:])

    def __init__(self, *fields):
        pass

    @abstractmethod
    def pack(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def pack_into(self, buffer: WriteableBuffer, offset: int):
        raise NotImplementedError


class FreePage(Page):
    MAGIC = b"\xdcFREEPG\xba"
    STRUCT = Struct("! 8s I 4084x")

    def __init__(self, next_free_pageno):
        super().__init__()
        self.next_free_pageno = next_free_pageno

    def pack(self) -> bytes:
        return self.STRUCT.pack(
            self.MAGIC,
            self.next_free_pageno,
        )

    def pack_into(self, buffer: WriteableBuffer, offset: int):
        self.STRUCT.pack_into(
            buffer,
            offset,
            self.MAGIC,
            self.next_free_pageno,
        )


class OverflowPage(Page):
    """
    TODO: It might be worthwhile to write the current offset into the page header
          as a sanity check.
    """

    MAGIC = b"oVeRfLoW"
    STRUCT = Struct("! 8s I 4084x")
    # this is the size of the data portion of an overflow page
    HEADERSIZE = 12
    DATASIZE = PAGESIZE - HEADERSIZE

    def __init__(self, next_overflow_pageno, overflow_data):
        super().__init__()
        self.next_overflow_pageno = next_overflow_pageno
        self.overflow_data = overflow_data

    def pack(self) -> bytes:
        return self.STRUCT.pack(
            self.MAGIC,
            self.next_overflow_pageno,
            self.overflow_data,
        )

    def pack_into(self, buffer: WriteableBuffer, offset: int):
        self.STRUCT.pack_into(
            buffer,
            offset,
            self.MAGIC,
            self.next_overflow_pageno,
            self.overflow_data,
        )

    def read_start(self, offset: int) -> Tuple[bytes, int]:
        """Read the data from offset

        Returns the data read so far, and how much needs to be read from the next
        overflow page.

        Offsets are relative to the end of the header, but because we store the data in
        a separate array, we don't have to do any math.
        """

        # FIXME: use a destination WriteableBuffer instead of return `bytes`.

        # We need to read the size of the data.
        # If there isn't at least 4 bytes left on the page, something is wrong.
        if offset >= self.DATASIZE - 4:
            raise ValueError("offset too close to end of overflow page")

        size = unpack_from("!I", self.overflow_data, offset)[0]

        # advance over the size data
        offset += 4

        return self.read_continue(offset, size)

    def read_continue(self, offset: int, size: int) -> Tuple[bytes, int]:
        """Continue a read begun by `read_start`.

        Retuns the data we were table to read from the current page,
        and the amount of data that needs to be read from the next overflow page.

        Allocating a new overflow page (if necessary) is the caller's
        responsilibity.
        """
        if offset == self.DATASIZE:
            return (b"", size)  # all the data is on the next page.
        if offset + size > self.DATASIZE:
            # read all the data on this page, but there is more to be read.
            data = self.overflow_data[offset : self.DATASIZE]
            return (data, offset + size - self.DATASIZE)
        else:
            # read the remaining data
            data = self.overflow_data[offset : offset + size]
            return (data, 0)

    def write_start(
        self, data: ReadableBuffer, offset: int
    ) -> Optional[ReadableBuffer]:
        # FIXME: read_start expects at least 4 bytes in the first page.
        #        We need to account for that here somehow.

        # prefix the data with its length
        data = length_prefix(data)
        end = offset + len(data)

        if end > self.DATASIZE:
            # the data doesn't fit on this page alone.
            # split it and return what we can't fit.
            split = self.DATASIZE - offset
            self.overflow_data[offset:split] = data[:split]
            return data[split:]

        # the data fits
        self.overflow_data[offset:end] = data
        return None  # no more data to write!

    def write_continue(self, data: ReadableBuffer) -> Optional[ReadableBuffer]:
        """Continue writing incomplete data.

        Offset is always zero. If we're continuing to write, that means we've
        filled a page and had to start a new one.

        Write everything we can, return the rest. It's the caller's
        responsibility to create a new overflow page if necessary.
        """
        # FIXME: record page offset after write?!?!?

        if len(data) <= self.DATASIZE:
            self.overflow_data[: len(data)] = data
            return None
        else:
            self.overflow_data[: len(data)] = data[: self.DATASIZE]
            return data[self.DATASIZE :]


class ZeroPage(OverflowPage):
    """
    The zero page of the database acts as an overflow page. We inherit from OverflowPage
    but add a larger header.
    """

    MAGIC = b"\xabZEROPG\xcd"
    STRUCT = Struct("! 8s I I I I H 4070s")
    HEADERSIZE = 26
    DATASIZE = PAGESIZE - HEADERSIZE

    def __init__(
        self,
        root_pageno=0,
        next_free_pageno=0,
        next_overflow_pageno=0,
        current_overflow_pageno=0,
        current_overflow_offset=0,  # header relative
        overflow_data=None,
    ):
        if overflow_data is None:
            self.overflow_data = bytearray(4074)
        else:
            self.overflow_data = bytearray(overflow_data)
        super().__init__(next_overflow_pageno, overflow_data)
        self.root_pageno = root_pageno
        self.next_free_pageno = next_free_pageno
        # self.next_overflow_pageno = next_overflow_pageno
        self.current_overflow_pageno = current_overflow_pageno
        self.current_overflow_offset = current_overflow_offset

    def pack(self) -> bytes:
        return self.STRUCT.pack(
            self.MAGIC,
            self.root_pageno,
            self.next_free_pageno,
            self.next_overflow_pageno,
            self.current_overflow_pageno,
            self.current_overflow_offset,
            self.overflow_data,
        )

    def pack_into(self, buffer: WriteableBuffer, offset: int):
        self.STRUCT.pack_into(
            buffer,
            offset,
            self.MAGIC,
            self.root_pageno,
            self.next_free_pageno,
            self.next_overflow_pageno,
            self.current_overflow_pageno,
            self.current_overflow_offset,
            self.overflow_data,
        )

    # TODO: we need to add the read/write methods from overflow page to this page.
    #       maybe instead have ZeroPage and OverflowPage inherit from same subclass.


class Pager:
    """Abstract all file reads and writes. Manage free space.

    TODO: add root entry to zero page
    TODO: support concurrent access

    Zero Page Format:
        u8[8]       ZERO_MAGIC
        u32         next free pageno
        u32         next overflow pageno
        u32         current overflow pageno (initially 0)
        u16         current overflow offset (initially 22)
        u8[4074]    overflow

    Free Page Format:
        u8[8]      FREE_MAGIC
        u32        next free pageno
        u8[4080]   padding
    """

    PAGESIZE = 0x1000
    ZERO_MAGIC = b"\xabZEROPG\xcd"  # 8 bytes
    FREE_MAGIC = b"\xdcFREEPG\xba"  # 8 bytes
    assert len(ZERO_MAGIC) == len(FREE_MAGIC)
    PAGE_FORMAT = Struct(f"!8sQ{PAGESIZE-16}x")
    assert PAGE_FORMAT.size == PAGESIZE

    @staticmethod
    def open_file(filepath: Union[AnyStr, os.PathLike]):
        if os.path.exists(filepath):
            mode = "r+b"
        else:
            mode = "w+b"
        return Pager(open(filepath, mode))

    def __init__(self, file: BinaryIO):
        """Create a new pager for `file`.

        `file` should either be blank or have a zero page.
        Anything else is an error.
        """
        if file.closed or not (file.readable() and file.writable()):
            raise ValueError(
                "Pager requires a file that is open, readable, and writeable"
            )
        self.file = file
        if self._seek_end() == 0:
            # blank file, create zero page.
            page = self.PAGE_FORMAT.pack(self.ZERO_MAGIC, 0)
            file.write(page)

        self._read_zero_page()
        self.cache = LRUCache(maxsize=32)

    def _seek_end(self) -> int:
        """Seek to the end of the file and return the position."""
        return self.file.seek(0, os.SEEK_END)

    def _seek_page(self, pageno: int):
        """Seek to the page given by `pageno`."""
        offset = pageno * self.PAGESIZE
        end = self._seek_end()
        if offset + self.PAGESIZE > end:
            raise ValueError("pageno out of bounds")
        self.file.seek(offset)

    def _read_zero_page(self):
        self._seek_page(0)
        data = self.PAGE_FORMAT.unpack(self.file.read(self.PAGESIZE))
        if data[0] != self.ZERO_MAGIC:
            raise RuntimeError("Bad MAGIC on zero page")
        self.next_free_pageno = data[1]

    def read_page(self, pageno: int, use_cache=True) -> bytes:
        """Fetch a page from the file.

        :param pageno: The page number to fetch.
        :type pageno: int
        :returns: The contents of the page as a bytes object.
        :rtype: bytes
        """

        if use_cache:
            data = self.cache.get(pageno)
            if data is not None:
                return data

        self._seek_page(pageno)
        data = self.file.read(self.PAGESIZE)
        assert len(data) == self.PAGESIZE

        self.cache.set(pageno, data)

        return data

    def write_page(self, pageno: int, data: bytes):
        """Write new page data.

        :param pageno: The pageno to write to.
        :type pageno: int
        :param data: The data to write.
        :type data: bytes
        """

        assert len(data) == self.PAGESIZE

        self._seek_page(pageno)
        count = self.file.write(data)
        if count != self.PAGESIZE:
            raise IOError("Incomplete page write")
        self.file.flush()
        self.cache.delete(pageno)

    def alloc_page(self) -> int:
        """Allocate a new page.

        :return: The page number of the new page.
        :rtype: int
        """

        if self.next_free_pageno != 0:
            # we have a previously allocated page we can use.
            pageno = self.next_free_pageno
            data = self.PAGE_FORMAT.unpack(self.read_page(pageno))
            if data[0] != self.FREE_MAGIC:
                raise RuntimeError("invalid free page format: bad magic")
            self._write_next_free_pageno(data[1])
        else:
            pageno = self._alloc_fresh_page()

        return pageno

    def _alloc_fresh_page(self) -> int:
        """Allocate a fresh page at the end of the file."""

        # find the end of the file
        next_page = self._seek_end()

        # align with page boundary
        if next_page % self.PAGESIZE != 0:
            next_page = (next_page & ~(self.PAGESIZE - 1)) + self.PAGESIZE
            assert next_page % self.PAGESIZE == 0
            # next_page is an offset not a pageno!!!
            self.file.seek(next_page)

        # write a blank page
        self.file.write(b"\x00" * self.PAGESIZE)

        # return the page number
        return next_page // self.PAGESIZE

    def free_page(self, pageno: int):
        """Free the given page."""

        # clear the cache
        self.cache.delete(pageno)

        # clear the page and write the pointer to the next free page.
        data = self.PAGE_FORMAT.pack(self.FREE_MAGIC, self.next_free_pageno)
        self.write_page(pageno, data)
        # commit the next free page to the zero page.
        self._write_next_free_pageno(pageno)

    def _write_next_free_pageno(self, pageno: int):
        """Commit the first free pageno to the zero page."""
        data = self.PAGE_FORMAT.pack(self.ZERO_MAGIC, pageno)
        self.write_page(0, data)
        self.next_free_pageno = pageno

    def close(self):
        """Close the pager and its underlying file object."""
        self.file.flush()
        self.file.close()

    def read_overflow(self, pageno: int, offset: int) -> bytes:
        """Read the overflow data that begins at pageno and offset."""
        # FIXME: use buffers instead of appending `bytes` objects.

        # fetch the overflow page
        page = OverflowPage.from_page(self.get_page(pageno))
        data = bytes()

        # read everything we can from the first page.
        current_data, togo = page.read_start(offset)
        data += current_data

        while togo > 0:
            # we still have data to read, so fetch the next overflow page.
            page = OverflowPage.from_page(self.get_page(page.next_overflow_pageno))
            # continue reading the data from the start of the next overflow page.
            current_data, togo = page.read_continue(0, togo)
            # append new data
            data += current_data

        return data

