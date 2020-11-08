"""Utilities
"""
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class Entry:
    """A simple dataclass that compares on the key and ignores the value.

    >>> Entry(1, 2) == Entry(1, 3)
    True
    >>> sorted([Entry(2, 1000), Entry(1, {}), Entry (3, None)])
    [Entry(key=1, value={}), Entry(key=2, value=1000), Entry(key=3, value=None)]
    """

    key: Any
    value: Any = field(compare=False)


def split_list(x):
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

    mid = len(x) // 2
    return (x[:mid], x[mid:])
