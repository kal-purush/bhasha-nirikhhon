import heapq
from abc import ABC, abstractmethod
from typing import Tuple, Hashable, TypeVar, Generic


REMOVED = "<removed-task>"  # placeholder for a removed task

K = TypeVar("K")
V = TypeVar("V", bound=Hashable)

class Heap(ABC, Generic[K, V]):
    """
    Simple heap interface. It doesn't allow duplicate items.

    At this interface level V shouldn't need to be Hashable. However, both implementations require it
    and I'm being lazy. To support item deletion, both implementations use items as dict keys. Thus
    unhashable types like lists and dicts can't be used. See: https://docs.python.org/3/glossary.html#term-hashable
    """

    @abstractmethod
    def insert(self, key: K, item: V) -> None:
        """
        Add a new item or update the key of an existing item
        """
        pass

    @abstractmethod
    def extract_min(self) -> Tuple[K, V]:
        """
        Remove the item with the smallest key. If there are multiple items with the smallest key, no
        guarantee is given about which item will be removed.

        :raises: IndexError if heap is empty
        :return: key, item
        """
        pass

    @abstractmethod
    def delete(self, item: V) -> None:
        """
        Remove an item

        :raises: KeyError if item not in heap
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        pass


class HeapEntry:
    def __init__(self, item: V):
        self.item = item

    def __lt__(self, other: V) -> bool:
        """
        Entries need to be comparable when using heapq for the heap algorithm. If there are duplicate keys,
        it compares the entries in order to determine sort order.
        """
        return True


class StandardLibHeap(Heap):
    """
    Heap implementation based on the heapq algorithm provided by the standard lib
    https://docs.python.org/3/library/heapq.html
    """

    def __init__(self):
        self.h = []
        self.entry_finder = {}

    def insert(self, key: K, item: V) -> None:
        """
        :raises TypeError if item unhashable
        """
        if item in self.entry_finder:
            self.delete(item)

        entry = HeapEntry(item)
        self.entry_finder[item] = entry
        heapq.heappush(self.h, (key, entry))

    def extract_min(self) -> Tuple[K, V]:
        key, entry = heapq.heappop(self.h)
        if entry.item == REMOVED:
            return self.extract_min()
        else:
            del self.entry_finder[entry.item]
            return key, entry.item

    def delete(self, item: V) -> None:
        entry = self.entry_finder[item]
        entry.item = REMOVED

    def is_empty(self) -> bool:
        for key, entry in self.h:
            if entry.item is not REMOVED:
                return False
        return True


class MyHeap(Heap):
    def insert(self, key: K, item: V) -> None:
        pass

    def extract_min(self) -> Tuple[K, V]:
        pass

    def delete(self, item: V) -> None:
        pass

    def is_empty(self) -> bool:
        pass