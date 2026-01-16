from lab3.linear_data_structures.collections import SortedList, T, ComparableNode


class SortedDoubleLinkedList(SortedList[T]):
    def __init__(self):
        super().__init__()

    def insert(self, e: T):
        new_node = ComparableNode[T](e)
        self.size += 1
        if self.is_empty():
            self.head = new_node
            self.tail = new_node
        elif self.head > new_node:
            self.head.previous = new_node
            new_node.next = self.head
            self.head = new_node
        else:
            node = self.head
            while node.next and node < new_node:
                node = node.next

            if node.next or node > new_node:
                new_node.previous = node.previous
                new_node.next = node
                node.previous.next = new_node
                node.previous = new_node
            else:
                node.next = new_node
                new_node.previous = node
                self.tail = new_node

    def delete(self, e: T):
        if self.is_empty():
            raise IndexError
        rm_node = ComparableNode[T](e)
        result = None
        node = self.head
        if node == rm_node:
            result = self.head
            self.head = None
            self.tail = None
            self.size -= 1
        else:
            while node != rm_node:
                node = node.next

            if node:
                self.size -= 1
                result = node
                node.previous.next = node.next
                if node.next:
                    node.next.previous = node.previous
                else:
                    self.tail = node.previous
            else:
                result = None

        return result.value if result else None