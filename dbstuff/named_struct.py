from typing import Any, Mapping
from collections import OrderedDict
from struct import Struct


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


CODES: Mapping[str, FieldType] = {}


def make_field_type(name, code):
    typ = type(name, (FieldType,), {"CODE": k})
    CODES[code] = typ
    return typ


Padding = make_field_type("Padding", "x")
Byte = make_field_type("Byte", "b")
UByte = make_field_type("UByte", "B")
Short = make_field_type("Short", "h")
UShort = make_field_type("UShort", "H")
Int = make_field_type('Int', 'i')
UInt = make_field_type('UInt', 'I')


def normalize_field(field):
    if isinstance(field, str):
        # TODO: parse out the count if there is one
        return CODES[field]()
    elif isinstance(field, FieldType):
        return field
    raise ValueError("Bad field type")


class NamedStruct:
    __fields: OrderedDict[str, FieldType]
    struct_format: str
    struct: Struct

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
        setattr(cls, "struct", Struct(struct_format))
        super().__init_subclass__(**kwargs)

    @classmethod
    def unpack(cls, data):
        # FIXME: this should return an instance of NamedStruct

        return cls.struct.unpack(data)

    def pack(self, *args) -> bytes:
        return self.struct.pack(*args)

    @property
    def size(self):
        return self.struct.size()


class MyStruct(NamedStruct, byteorder="!"):
    barf = "x"
    something = Byte(24)
    what = Padding(5)


print(MyStruct.struct_format)
