"""Abstract all file reads and writes. Manage free space."""

import os
from struct import Struct

from dbstuff.cache import LRUCache


class Pager:
    """Abstract all file reads and writes. Manage free space.

    TODO: support concurrent access
    """

    PAGESIZE = 0x1000
    ZERO_MAGIC = b"axdbbaby"
    FREE_MAGIC = b"freepage"
    PAGE_FORMAT = Struct(f"!8sQ{PAGESIZE-16}x")
    """Page Format:
    uint8_t[4]      MAGIC
    uint64_t        next free page
    uint8_t[]       padding
    """

    @staticmethod
    def open_file(filepath):
        if os.path.exists(filepath):
            mode = "r+b"
        else:
            mode = "w+b"
        return Pager(open(filepath, mode))

    def __init__(self, file):
        """Create a new pager for `file`.

        `file` should either be blank or have a zero page.
        Anything else is an error.
        """
        self.file = file
        end = self._seek_end()
        if end == 0:
            # blank file, create zero page.
            page = self.PAGE_FORMAT.pack(self.ZERO_MAGIC, 0)
            file.write(page)

        self._read_zero_page()
        self.cache = LRUCache(maxsize=32)

    def _seek_end(self):
        return self.file.seek(0, os.SEEK_END)

    def _seek_page(self, pageno):
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

    def read_page(self, pageno, use_cache=True):
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

    def write_page(self, pageno, data):
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

    def alloc_page(self):
        """Allocate a new page."""

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

    def _alloc_fresh_page(self):
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

    def free_page(self, pageno):
        """Free a page."""

        # clear the cache
        self.cache.delete(pageno)

        # clear the page and write the pointer to the next free page.
        data = self.PAGE_FORMAT.pack(self.FREE_MAGIC, self.next_free_pageno)
        self.write_page(pageno, data)
        # commit the next free page to the zero page.
        self._write_next_free_pageno(pageno)

    def _write_next_free_pageno(self, pageno):
        data = self.PAGE_FORMAT.pack(self.ZERO_MAGIC, pageno)
        self.write_page(0, data)
        self.next_free_pageno = pageno

    def close(self):
        self.file.flush()
        self.file.close()
