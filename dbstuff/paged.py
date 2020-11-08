from array import array
from struct import Struct


def bytes2u64(b):
    return int.from_bytes(b.ljust(8, b"\0"), "big", signed=False)


def slice_key(k: str) -> [bytes]:
    b = k.encode("utf-8")
    slices = array("Q")
    for i in range(0, len(b), 8):
        slices.append(bytes2u64(b[i : i + 8]))
    return slices


class InteriorNode:
    """
    Interior page
      uint16_t      MAGIC
      uint8_t       page type (1 for interior nodes)
      uint8_t       Number of keys
      uint8_t[4]    zero
      uint64_t[255] key_slices
      uint64_t[256] child_pointers
    """

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
