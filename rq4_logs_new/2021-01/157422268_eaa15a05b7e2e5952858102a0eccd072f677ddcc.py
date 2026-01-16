class LinkedList:
    def __init__(self, value, next_item: 'LinkedList'=None):
        self.value = value
        self._next_item: LinkedList = next_item

    def set(self, item: 'LinkedList') -> 'LinkedList':
        old = self._next_item
        self._next_item = item
        return self._next_item

    def append(self, item: 'LinkedList') -> 'LinkedList':
        if self._next_item is None:
            self._next_item = item
            return self
        return self._next_item.append(item)

    def __getitem__(self, key: int) -> LinkedList:
        current = self
        for i in range(key):
            current = current._next_item
        return current

    def __iter__(self):
        raise ValueError