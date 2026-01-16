class Node:

    def __init__(self, data):
        self.data = data
        self.next_ = None


class LinkedList:

    def __init__(self):
        self._head = None
        self._count = 0

    def add(self, item):
        node = Node(item)
        if self._head is None:
            self._head = node
        else:
            p = self._head
            while p.next_ is not None:
                p = p.next_
            p.next_ = node
        self._count += 1

    def remove_head(self):
        item = self._head.data
        next_ = self._head.next_
        self._head = next_
        self._count -= 1
        return item

    def remove_tail(self):
        if not self._head.next_:
            data = self._head.data
            self._head = None
            self._count -= 1
            return data

        next_ = self._head
        penult = None

        while next_.next_ is not None:
            penult = next_
            next_ = next_.next_
        penult.next_ = None
        self._count -= 1
        return next_.data

    def head(self):
        return self._head.data

    def tail(self):
        p = self._head

        while p.next_ is not None:
            p = p.next_

        return p.data

    def search(self, data):
        next_ = self._head
        found = None

        while next_:
            if next_.data == data:
                found = data
                break
            next_ = next_.next_
        return found

    def __len__(self):
        return self._count
