from random import shuffle
from dbstuff.memory import BPlusTree


def test_single():
    bt = BPlusTree()
    bt[1] = "a"
    assert bt[1] == "a"
    del bt[1]
    assert bt[1] is None


def test_random():
    N = 1000

    r = list(range(1, N + 1))
    shuffle(r)

    bt = BPlusTree()

    for i in r:
        bt[i] = i * 1000

    shuffle(r)
    for i in r:
        x = bt[i]
        assert x == i * 1000

    shuffle(r)
    for i in r:
        del bt[i]
        assert bt[i] is None
