from typing import Any, Mapping
from collections import OrderedDict


# PEP 520
# https://www.python.org/dev/peps/pep-0520/


class FieldType:
    CODE: str

    def __init__(self, count: int = 1, /, default=None):
        self.count: int = count
        self.default: Any = default

    def __str__(self):
        if self.count == 1:
            return self.CODE
        return str(self.count) + self.CODE


CODES: Mapping[str, Any] = {
    "x": "Padding",
    "b": "Byte",
    "B": "UByte",
    "h": "Short",
    "H": "UShort",
}

module = globals()
for k, v in CODES.items():
    typ = type(v, (FieldType,), {"CODE": k})
    module[v] = typ
    CODES[k] = typ

"""
# fmt: off
class Padding(FieldType, CODE="x"): pass  # noqa: E701
class Byte(FieldType, CODE="b"): pass  # noqa: E701
class UByte(FieldType, CODE="B"): pass  # noqa: E701
class Short(FieldType, CODE="h"): pass  # noqa: E701
class UShort(FieldType, CODE="H"): pass  # noqa: E701
"""


def normalize_field(field):
    if isinstance(field, str):
        return CODES[field]()
    return field


class Struct:
    __fields: OrderedDict[str, FieldType]
    struct_format: str

    def __init_subclass__(cls, byteorder="", **kwargs) -> None:
        fields = OrderedDict()
        struct_format = [byteorder]
        for name in cls.__dict__:
            if not name.startswith("_"):
                field = normalize_field(cls.__dict__[name])
                fields[name] = field
                setattr(cls, name, field)
                struct_format.append(str(field))
        setattr(cls, "__fields", fields)
        setattr(cls, "struct_format", "".join(struct_format))
        super().__init_subclass__(**kwargs)


class MyStruct(Struct, byteorder="!"):
    barf = "x"
    something = Byte(24)
    what = Padding(5)


print(MyStruct.struct_format)
