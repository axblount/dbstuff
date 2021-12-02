"""
BPlusTree stored on disk.
"""


from __future__ import annotations
from bisect import bisect_left, bisect_right
from math import ceil
from typing import Generic, List, Optional, Tuple, TypeVar
from dbstuff.util import Entry, split_list
from dbstuff.pager import Pager
from enum import Enum


# These are the signals returned by child nodes after performing an operation.
# REBALANCE on insert means the child is too full.
# REBALANCE on delete means the child is too empty.
class ChildStatus(Enum):
    DONE = "DONE"
    REBALANCE = "REBALANCE"


K = TypeVar("K")  # key
V = TypeVar("V")  # value


class BPlusTree(Generic[K, V]):

    def __init__(self, order: int, pager: Pager):
        if order < 3:
            raise ValueError("BPlusTree order must be >= 3")
        self.ORDER = order
        self.MAX_KEYS = self.ORDER - 1
        self.MIN_KEYS = ceil(self.MAX_KEYS / 2)

        self.root = InteriorNode(self)
        self.root.children = [LeafNode(self, None, None)]

    @property
    def first_leaf(self):
        """The leaf with the smallest keys."""
        node = self.root
        while isinstance(node, InteriorNode):
            node = node.children[0]
        return node

    @property
    def last_leaf(self):
        """The leaf with the largest keys."""
        node = self.root
        while isinstance(node, InteriorNode):
            node = node.children[-1]
        return node

    def __setitem__(self, key: K, value: V):
        result = self.root.insert(key, value)
        if result == ChildStatus.REBALANCE:
            median, right_child = self.root.split()
            new_root = InteriorNode(self)
            new_root.keys = [median]
            new_root.children = [self.root, right_child]
            self.root = new_root

    def __getitem__(self, key: K) -> Optional[V]:
        dummy = Entry(key, None)
        leaf = self.root.find_leaf(key)
        if dummy in leaf.entries:
            i = leaf.entries.index(dummy)
            return leaf.entries[i].value
        return None

    def __contains__(self, key: K) -> bool:
        dummy = Entry(key, None)
        leaf = self.root.find_leaf(key)
        return dummy in leaf.entries

    def __delitem__(self, key: K):
        self.root.delete(key)
        if len(self.root.keys) == 0 and isinstance(
            self.root.children[0], InteriorNode
        ):
            self.root = self.root.children[0]

    def show(self):
        self.root.show()


class InteriorNode(Generic[K, V]):
    """A B+-tree interior (non-leaf) node."""

    def __init__(self, tree: BPlusTree[K, V]):
        self.tree = tree
        self.keys: List[K] = []
        self.children = []

    def show(self, level=0):
        print("  " * level, "=>", self.keys)
        for c in self.children:
            c.show(level + 1)

    def find_leaf(self, key: K) -> LeafNode:
        ii = -1
        for i, k in enumerate(self.keys):
            if key < k:
                ii = i
                break
        return self.children[ii].find_leaf(key)

    def split(self) -> Tuple[K, InteriorNode[K, V]]:
        """creates a new right sibling and moves half the keys over"""
        assert len(self.keys) == self.tree.MAX_KEYS + 1
        right_sib = InteriorNode(self.tree)
        median = self.keys.pop(ceil(len(self.keys) / 2 - 1))
        self.keys, right_sib.keys = split_list(self.keys)
        self.children, right_sib.children = split_list(self.children)
        assert len(self.keys) + 1 == len(self.children)
        assert len(right_sib.keys) + 1 == len(right_sib.children)
        return median, right_sib

    def insert(self, key: K, value: V) -> ChildStatus:
        i = bisect_right(self.keys, key)
        child = self.children[i]
        result = child.insert(key, value)

        if result == ChildStatus.REBALANCE:
            median, right_child = child.split()
            self.keys.insert(i, median)
            self.children.insert(i + 1, right_child)

            if len(self.keys) > self.tree.MAX_KEYS:
                return ChildStatus.REBALANCE

        return ChildStatus.DONE

    def delete(self, key: K) -> ChildStatus:
        # which index to descend?
        i = bisect_right(self.keys, key)
        child = self.children[i]

        # delete from child tree and see what we need to do.
        result = child.delete(key)
        if result == ChildStatus.REBALANCE:
            self.rebalance_after_delete(i)
            if len(self.keys) < self.tree.MIN_KEYS:
                return ChildStatus.REBALANCE
        return ChildStatus.DONE

    def rebalance_after_delete(self, i: int):
        """Rebalance child `i` after a deletion

        Try to borrow,otherwise merge.
        """

        child = self.children[i]
        assert child.is_minimal()

        # find right sibling
        right_sib = None
        if i < len(self.keys):
            right_sib = self.children[i + 1]

        # find left sibling
        left_sib = None
        if i > 0:
            left_sib = self.children[i - 1]

        # try to borrow from a sibling
        if right_sib is not None and not right_sib.is_minimal():
            # we can borrow!
            self.keys[i] = child.borrow_right(self.keys[i], right_sib)
        elif left_sib is not None and not left_sib.is_minimal():
            # leaf borrow
            self.keys[i - 1] = child.borrow_left(self.keys[i - 1], left_sib)
        # we tried to borrow and couldn't: MERGE
        elif right_sib is not None:
            # self.keys[i] is the keyslice between child and right_sib
            new_child = child.merge(self.keys[i], right_sib)
            self.children[i] = new_child
            del self.children[i + 1]
            del self.keys[i]
        elif left_sib is not None:
            # self.keys[i-1] is the keyslice between child and left_sib
            new_child = left_sib.merge(self.keys[i - 1], child)
            self.children[i - 1] = new_child
            del self.children[i]
            del self.keys[i - 1]
        else:
            # only root should reach this
            assert self == self.tree.root

        assert len(self.keys) == len(self.children) - 1

    def borrow_right(self, median: K, right_sib: InteriorNode[K, V]) -> K:
        """
        borrow from right sib
        return the new median for parent to use
        """
        assert not right_sib.is_minimal()

        borrowed_child = right_sib.children.pop(0)
        new_median = right_sib.keys.pop(0)
        self.keys.append(median)
        self.children.append(borrowed_child)

        return new_median

    def borrow_left(self, median: K, left_sib: InteriorNode[K, V]) -> K:
        """
        borrow from left sib
        return the new median for parent to use
        """
        assert not left_sib.is_minimal()

        borrowed_child = left_sib.children.pop()
        new_median = left_sib.keys.pop()
        self.keys.insert(0, median)
        self.children.insert(0, borrowed_child)

        return new_median

    def merge(
        self, median: K, right_sib: InteriorNode[K, V]
    ) -> InteriorNode[K, V]:
        """
        merge with the right sibling.
        return a pointer to the new child.
        `median` is the keyslice that falls between
        self and right_sib
        """
        assert right_sib.is_minimal()

        self.keys.append(median)
        self.keys.extend(right_sib.keys)
        self.children.extend(right_sib.children)

        return self

    def is_minimal(self) -> bool:
        return len(self.keys) <= self.tree.MIN_KEYS


class LeafNode(Generic[K, V]):
    """A B+-tree leaf node containing keys and data."""

    def __init__(
        self,
        tree: BPlusTree[K, V],
        prev_leaf: Optional[LeafNode[K, V]],
        next_leaf: Optional[LeafNode[K, V]],
    ):
        self.tree = tree
        self.prev_leaf = prev_leaf
        self.next_leaf = next_leaf
        self.entries: List[Entry[K, V]] = []

    def show(self, level=0):
        print("  " * level, self.entries)

    def find_leaf(self, _key: K) -> LeafNode[K, V]:
        return self

    def split(self) -> Tuple[K, LeafNode[K, V]]:
        """creates a new right sibling and moves half its keys over"""
        right_sib = LeafNode(self.tree, self, self.next_leaf)
        if right_sib.next_leaf:
            right_sib.next_leaf.prev_leaf = right_sib
        self.next_leaf = right_sib

        self.entries, right_sib.entries = split_list(self.entries)
        median = right_sib.entries[0].key
        return median, right_sib

    def insert(self, key: K, value: V) -> ChildStatus:
        """
        Insert the key and value into this leaf.
        Notify the parent of the result.
        We either:
            1. Are done
            2. Split
        """

        e = Entry(key, value)
        i = bisect_left(self.entries, e)

        if e in self.entries:
            # replace the old entry
            self.entries[i] = e
        else:
            # insert the entry
            self.entries.insert(i, e)

        if len(self.entries) > self.tree.MAX_KEYS:
            return ChildStatus.REBALANCE

        # inserted without a problem
        return ChildStatus.DONE

    def delete(self, key: K) -> ChildStatus:
        """
        Remove the entry given by key
        If we're deficient, signal to the parent by returning
        REBALANCE
        """
        dummy = Entry(key, None)
        i = bisect_left(self.entries, dummy)

        if self.entries[i] == dummy:
            del self.entries[i]
            if len(self.entries) < self.tree.MIN_KEYS:
                return ChildStatus.REBALANCE
        return ChildStatus.DONE

    def borrow_right(self, _median: K, right_sib: LeafNode[K, V]) -> K:
        """
        borrow from right sib
        return the new keyslice for parent to use
        """
        assert self.next_leaf == right_sib
        assert right_sib.prev_leaf == self

        e = right_sib.entries.pop(0)
        self.entries.append(e)
        return right_sib.entries[0].key

    def borrow_left(self, _median: K, left_sib: LeafNode[K, V]) -> K:
        """
        borrow from left sib
        return the new keyslice for parent to use
        """
        assert left_sib.next_leaf == self
        assert self.prev_leaf == left_sib

        e = left_sib.entries.pop()
        self.entries.insert(0, e)
        return self.entries[0].key

    def merge(self, _median: K, right_sib: LeafNode[K, V]) -> LeafNode[K, V]:
        """
        merge with the right sibling.
        return a pointer to the new child.
        """
        assert self.next_leaf == right_sib
        assert right_sib.prev_leaf == self

        self.entries.extend(right_sib.entries)
        self.next_leaf = right_sib.next_leaf
        if self.next_leaf:
            self.next_leaf.prev_leaf = self
        return self

    def is_minimal(self) -> bool:
        return len(self.entries) <= self.tree.MIN_KEYS
