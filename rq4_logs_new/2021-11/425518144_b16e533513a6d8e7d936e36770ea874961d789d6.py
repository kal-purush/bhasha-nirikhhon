from typing import Any
class Node:
    def __init__(self, value = None, next = None):
        self.value = value
        self.next = next
        
        
class Queue:
    def __init__(self):
        self.head = None
        self.tail = None
    
    def peek(self) -> Any:
        return self.head.value
    
    def enqueue(self, element: Any) -> None:
        new_el = Node(element)
        if self.head is None:
            self.head = new_el
            self.tail = new_el
        else:
            self.tail.next = new_el
            self.tail = new_el
    
    def dequeue(self) -> Any:
        del_el = self.head
        self.head = None
        self.head = del_el.next
        return del_el.value
    
    def __repr__(self):
        x = ''
        temp = self.head
        while temp is not None:
            x = x + ', ' + str(temp.value)
            temp = temp.next
            
        x = x[2:]
        return x
    
    def __len__(self):
        q_len = 0
        temp = self.head
        while temp is not None:
            q_len += 1
            temp = temp.next
        return q_len 
    
    
queue = Queue()
assert len(queue) == 0
print(queue)
queue.enqueue('klient1')
queue.enqueue('klient2')
queue.enqueue('klient3')
assert str(queue) == 'klient1, klient2, klient3'
print(queue)
client_first = queue.dequeue()
assert client_first == 'klient1'
print(queue)
assert str(queue) == 'klient2, klient3'
print(queue)
assert len(queue) == 2
print(queue)