# Database related code

[![GitHub](https://img.shields.io/github/license/axblount/database-stuff?style=plastic)](COPYING.txt)

## "Done"

- In-memory B+-tree ([`dbstuff.memory.BPlusTree`](dbstuff/memory.py))
- Pager ([`dbstuff.pager.Pager`](dbstuff/pager.py))
- LRU cache with graveyard ([`dbstuff.cache.LRUCache`](dbstuff/cache.py))

## TODO

### Paged B+-tree
I need to decide how to pack keys and child_pointers for variable length keys.
So far...
- Keys with length &leq;255: length byte, entire key
- Keys with length >255: zero byte, four byte overflow pageno, 2 byte offset, first 249 bytes of key

## References
