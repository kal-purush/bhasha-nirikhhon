from typing import Any, List, TypeVar, Generic

import abc
import random

T = TypeVar("T")


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any):
        pass


class PearsonHasher(Hasher):
    _table: bytes

    def __init__(self, table_size: int = 1024):
        self._table = random.randbytes(table_size)

    def hash(self, data: Any) -> int:
        h = 0
        ba = bytes(data, 'utf-8') if type(data) == str else bytes(data)

        for byte in ba:
            idx = h ^ byte
            h = self._table[idx % len(self._table)]

        return h


class HashTable(Generic[T]):
    _storage: List[T]
    _hasher: Hasher

    hash_miss: int

    def __init__(self, cap: int = 100):
        self._storage = cap * [None]
        self._hasher = PearsonHasher()
        self.hash_miss = 0

    def __len__(self):
        return len(self._storage)

    def add(self, value: T):
        index = self._hasher.hash(value) % len(self._storage)
        if self._storage[index] is not None:
            self.hash_miss += 1
        self._storage[index] = value