"""
https://leetcode.com/problems/lru-cache/
Design and implement a data structure for Least Recently Used (LRU) cache. It should support the following operations: get and put.

get(key) - Get the value (will always be positive) of the key if the key exists in the cache, otherwise return -1.
put(key, value) - Set or insert the value if the key is not already present. When the cache reached its capacity, it should invalidate the least recently used item before inserting a new item.

The cache is initialized with a positive capacity.

Follow up:
Could you do both operations in O(1) time complexity?
"""

class LRUCache:

    def __init__(self, capacity: int):
        self.cache = {}
        self.limit = capacity

    def update(self, key: int, value: int):
        del [self.cache[key]]
        self.cache[key] = value

    def get(self, key: int) -> int:
        if self.cache.get(key):
            self.update(key, self.cache[key])
            print(self.cache[key])
        else:
            print(-1)

    def put(self, key: int, value: int) -> None:
        if not self.cache.get(key):
            if len(self.cache) < self.limit:
                self.cache[key] = value
            else:
                for k in self.cache:
                    del[self.cache[k]]
                    break
                self.cache[key] = value
        else:
            self.update(key, value)

cache = LRUCache(2)

cache.put(2, 1)
cache.put(1, 1)
cache.put(2, 3)
cache.put(4, 1)
cache.get(1)
cache.get(2)
