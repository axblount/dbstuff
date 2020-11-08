import pytest
import tempfile
from dbstuff.pager import Pager


BLANK_PAGE = b"\0" * 4096


@pytest.fixture(scope="function")
def new_pager():
    file = tempfile.TemporaryFile("rb+")
    return Pager(file)


def test_sanity():
    assert Pager.PAGE_FORMAT.size == Pager.PAGESIZE


def test_create(new_pager):
    data = new_pager.read_page(0)
    # 1. check the magic number
    # 2. Since we've never allocated or freed a page,
    #    the next free page pointer should be 0
    assert data == Pager.PAGE_FORMAT.pack(Pager.ZERO_MAGIC, 0)


def test_alloc_page(new_pager):
    pageno = new_pager.alloc_page()
    assert pageno == 1
    data = new_pager.read_page(pageno)
    assert data == BLANK_PAGE
    data = b"\xff" * Pager.PAGESIZE
    new_pager.write_page(pageno, data)
    assert new_pager.read_page(pageno) == data


def test_read_oob_pageno(new_pager):
    with pytest.raises(ValueError, match="pageno out of bounds"):
        new_pager.read_page(1)


def test_write_oob_pageno(new_pager):
    with pytest.raises(ValueError, match="pageno out of bounds"):
        new_pager.write_page(1, BLANK_PAGE)


def test_free_page(new_pager):
    pageno = new_pager.alloc_page()
    assert pageno == 1
    new_pager.write_page(pageno, b"\x88" * Pager.PAGESIZE)
    new_pager.free_page(pageno)
    assert new_pager.next_free_pageno == 1
    data = new_pager.read_page(new_pager.next_free_pageno)
    # check the magic number that marks this as a free page
    # since this is the only free page, the next_free_page pointer should be 0.
    assert data == Pager.PAGE_FORMAT.pack(Pager.FREE_MAGIC, 0)


def test_create_close_open(tmp_path):
    path = tmp_path / "temp.bin"
    dummy_data = b"\xab\xcd" * (Pager.PAGESIZE // 2)

    # create file and write some data
    pager = Pager.open_file(path)

    # allocate a page and write some data to it.
    pageno = pager.alloc_page()
    assert pageno == 1
    assert pager.next_free_pageno == 0
    pager.write_page(pageno, dummy_data)
    data = pager.read_page(pageno)
    assert data == dummy_data

    # allocate a second page and free it
    pageno = pager.alloc_page()
    assert pageno == 2
    pager.free_page(pageno)
    assert pager.next_free_pageno == 2

    pager.close()

    # re-open the file
    pager = Pager.open_file(path)
    assert pager.next_free_pageno == 2
    # read the same page as before
    data = pager.read_page(1)
    assert data == dummy_data
    # allocate a new page, we should get the previously freed page
    pageno = pager.alloc_page()
    assert pageno == 2
    assert pager.next_free_pageno == 0
