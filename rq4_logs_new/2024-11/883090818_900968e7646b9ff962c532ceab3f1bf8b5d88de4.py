class Node:

    def __init__(self, value):
        self.value = value
        self.next = None

class LinkedList:

    def __init__(self):
        self.head = None
        self.size = 0

    def __repr__(self) -> str:
        if self.head is None:
            return "[]"
        else:
            last = self.head

            return_string = f"[{last.value}"

            while last.next:
                last = last.next
                return_string += f", {last.value}"
            return_string += "]"

            return return_string
        
    def __contains__(self, value):
        last = self.head
        while last is not None:
            if last.value == value:
                return True
            last = last.next
        return False

    def __len__(self):
        last = self.head
        counter = 0
        while last is not None:
            counter += 1
            last = last.next
        return counter

    def append(self, value):
        if self.head is None:
            self.head = Node(value)
            self.size = 1
        else:
            last = self.head
            while last.next:
                last = last.next
            last.next = Node(value)
            self.size += 1
        
    def prepend(self, value):
        first_node = Node(value)
        first_node.next = self.head
        self.head = first_node
        self.size += 1

    def insert(self, value, index):
        self.size += 1

        if index == 0:
            self.prepend(value)
        else:
            if self.head is None:
                raise ValueError("Index out of bounds")
            else:
                last = self.head

                for i in range(index-1):
                    if last.next is None:
                        raise ValueError("Index out of bounds")
                    last = last.next

                new_node = Node(value)
                new_node.next = last.next
                last.next = new_node

    def delete(self, value):
        last = self.head

        if last is not None:
            self.size -= 1
            if last.value == value:
                self.head = last.next
            else:
                while last.next:
                    if last.next.value == value:
                        last.next = last.next.next
                        break
                    last = last.next

    def pop(self, index):
        if self.head is None:
            raise ValueError("Index out of bounds")
        else:
            self.size -= 1
            last = self.head

            for i in range(index-1):
                if last.next is None:
                    raise ValueError("Index out of bounds")
                last = last.next
            
            if last.next is None:
                raise ValueError("Index out of bounds")
            else:
                last.next = last.next.next

    def get(self, index):
        if self.head is None:
            raise ValueError("Index out of bounds")
        else:
            last = self.head
            for i in range(index-1):
                if last.next is None:
                    raise ValueError("Index out of bounds")
                last = last.next
            return last.value

    def get_size(self):
        return f"size:  {self.size}"


if __name__ == "__main__":
    ll = LinkedList()

    ll.append(10)
    ll.append(5)
    ll.append(18)
    ll.append(22)
    ll.append(55)
    ll.append(13)
    ll.append(99)
    ll.append(30)

    ll.prepend(100)

    ll.insert(200, 1)

    ll.delete(18)

    ll.pop(1)

    print(ll)
    print(ll.get(1))
    print(99 in ll)
    print(880 in ll)
    print(ll.get_size())