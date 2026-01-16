

class Node:
    def __init__(self, data=None):
        self.data = data
        self.next = None

class singlyLinkedList:
        def __init__(self) -> None:
             self.tail = None
        
        def append(self, data):
            node = Node(data)

            if self.tail == None:
                self.tail = node
            else:
                current = self.tail
                while current.next:
                    current = current.next
                current.next = node



if __name__ == "__main__":
    print("hello, Mr12")