import pytest
from random import shuffle
from dbstuff.memory import BPlusTree


@pytest.mark.parametrize("order", range(3, 20))
def test_tree_random(order):
    r = list(range(order * 10))
    shuffle(r)

    bt = BPlusTree(order)

    for i in r:
        bt[i] = i

    shuffle(r)
    for i in r:
        x = bt[i]
        assert x == i

    shuffle(r)
    for i in r:
        del bt[i]
        assert bt[i] is None
