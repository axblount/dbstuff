"""A paged B+tree implementation.

The data is stored in 4kb pages.
Each page will be marked with a type and hold different types of data.
- InteriorNode
- LeafNode
- Overflow

How many keys fit for 32-bit child pointers? (Interior Node)
    k + 1 = c
    8k + 4c = 4096 - 8

    8k + 4(k+1) = 4088
    12k + 4 = 4088
    12k = 4084
    k ~ 340
    c ~ 341

64-bit pointers  in leaf node.
    8k + 8k = 4088
    16k = 4088
    k ~ 255

The formats are as follows:

InteriorPage `Struct('!BH5x340Q341I4x')`:
    u8       page type (0x01 for interior pages)
    u16      Number of keys
    u8[5]    padding
    u64[340] key_slices
    u32[341] child_pagenos
    u8[4]    padding

LeafPage `Struct('!BH5x340Q340I8x')`:
    u8       page type (0x02 for leaf pages)
    u16      Number of keys
    u8[5]    padding
    u64[340] keys
    u32[340] data_pointers
    u8[8]    padding

OverflowPage `Struct('!BI4091B')`:
    u8      page type (0x03 for overflow pages)
    u32     next overflow page
    u8[4091] data
"""

from array import ArrayType, array
from named_struct import NamedStruct, UShort, UByte, UInt


def bytes2u64(b: bytes) -> int:
    if len(b) > 8:
        raise ValueError("can only convert bytes of length 8 to an int.")
    return int.from_bytes(b.ljust(8, b"\0"), "big", signed=False)


def slice_key(k: str) -> ArrayType[int]:
    b = k.encode("utf-8")
    slices = array("Q")
    for i in range(0, len(b), 8):
        slices.append(bytes2u64(b[i : i + 8]))
    return slices


class PagedNode:
    @classmethod
    def from_page(cls, page_data):
        cls.PageFormat.unpack(page_data)


PAGE_SIZE = 4096

class InteriorNode:
    # New style:
    # Interior nodes have a header and a payload.
    # the payload will not have a fixed format and will need to be parsed out
    # as the node can have a variable number of keys with variable lengths.

    HEADER_SIZE = 5
    MAX_WEIGHT = PAGE_SIZE - HEADER_SIZE

    class PageFormat(NamedStruct, byteorder='!'):
        magic = UShort()
        page_type = UByte(default=1)
        weight = UShort()
        payload = UByte(PAGESIZE - 5)

    def __init__(self, keys: List[bytes], children: List[int]):
        assert len(children) == len(keys) + 1
        self.keys = keys
        self.children = children

    @property
    def weight(self):
        w = 0
        for k in keys:
            w += len(k)
        w += 4 * len(children)
        return w

    @staticmethod
    def from_page(page_data):
        PageFormat.unpack(page_data)


"""
class InteriorNode:
    \"""
    Interior page
      uint16_t      MAGIC
      uint8_t       page type (1 for interior nodes)
      uint8_t       Number of keys
      uint8_t[4]    zero
      uint64_t[255] key_slices
      uint64_t[256] child_pointers
    \"""

    page_format = Struct("!HBBxxxx255Q256Q")
    assert page_format.size == 4096, "InteriorNode doesn't fit in a page."
    MAGIC = 0x4447
    PAGE_TYPE = 0x01

    def __init__(self):
        self.nkeys = 0
        self.key_slices = array("Q")
        self.child_pointers = array("Q")

    def pack(self):
        b = self.page_format.pack(
            self.MAGIC,
            self.PAGE_TYPE,
            self.nkeys,
            *self.key_slices,
            *([0] * (255 - len(self.key_slices))),
            *self.child_pointers,
            *([0] * (256 - len(self.child_pointers))),
        )
        assert len(b) == 4096
        return b


# print(slice_key('Hello, World!'))
# InteriorNode().pack()
"""
