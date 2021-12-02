"""Data agnostic caching utilities.
"""

from dbstuff.util import ReadWriteLock
from weakref import WeakValueDictionary
from collections import OrderedDict


class LRUCache:
    """A Least-Recently-Used cache.

    The cache also includes a 'graveyard' of items that have been evicted from
    the cache. These are stored as weak references throught a
    `weakref.WeakValueDictionary`. Evicted items can be resurrected if they are
    retrieved before gc.

    I purposely chose to *not* implement __getitem__ and __setitem__ to distance
    this class from builtin dicts and similar objects.

    Based on:
    https://docs.python.org/3.9/library/collections.html#ordereddict-examples-and-recipes

    :param maxsize: The maximum number of items to store in the cache.

    >>> import gc
    >>> class A:
    ...     def __init__(self, x):
    ...         self.x = x
    ...     def __repr__(self):
    ...         return 'ALIVE ' + str(self.x)
    >>> c = LRUCache(maxsize=2)
    >>> c.set(1, A(1))
    >>> c.set(2, A(2))
    >>> c.set(3, A(3))
    >>> _ = gc.collect()
    >>> c.get(1, 'DEAD')
    'DEAD'
    >>> c.get(2)
    ALIVE 2
    >>> c.get(3)
    ALIVE 3
    >>> c.set(4, A(4))
    >>> _ = gc.collect()
    >>> c.get(2, 'DEAD')
    'DEAD'
    >>> c.hits
    2
    >>> c.misses
    2
    """

    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.lru = OrderedDict()
        self.grave = WeakValueDictionary()
        self.hits = 0
        self.misses = 0
        self.resurrections = 0
        self.rwlock = ReadWriteLock()

    def get(self, key, default=None):
        """Retrieve an item from the cache if it exists.

        :param key: The key to retrieve.
        :param default: The default value to return if the item doesn't exist.
        :return: The value associated with key.
        """

        with self.rwlock.read_access:
            try:
                value = self.lru[key]
                self.hits += 1
            except KeyError:
                # try to resurrect
                value = self.grave.pop(key, None)
                if value is not None:
                    self.lru[key] = value
                    self.resurrections += 1
                else:
                    self.misses += 1
                    return default

            self.lru.move_to_end(key)
        return value

    def set(self, key, value):
        """Add a value to the cache.

        :param key: The key for the item.
        :param value: The value to store.
        """
        with self.rwlock.write_access:
            if key in self.grave:
                del self.grave[key]

            if key in self.lru:
                self.lru.move_to_end(key)
            self.lru[key] = value

            while len(self.lru) > self.maxsize:
                # remove old items from the cache
                # send them to live with the dead
                (k, v) = self.lru.popitem(last=False)
                self.grave[k] = v

    def delete(self, key):
        """Remove a value from the cache.

        If there is no item associated with key, do nothing.

        :param key: The key to delete.
        """
        with self.rwlock.write_access:
            if key in self.grave:
                del self.grave[key]
            if key in self.lru:
                del self.lru[key]
