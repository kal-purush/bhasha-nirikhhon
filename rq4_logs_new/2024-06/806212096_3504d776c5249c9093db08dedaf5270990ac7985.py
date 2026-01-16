from numpy import random

class Node:
    def __init__(self, data):
        self.prev = None
        self.data = data
        self.next = None

    def __str__(self):
        return str(self.data)


class Double_LinkedList:
    def __init__(self):
        self.head = None

    
    def is_empty(self):
        return self.head == None

    
    def get_tail(self):
        if self.is_empty():
            return 

        last_node = self.head

        while last_node.next:
            last_node = last_node.next

        return last_node

    
    def append(self, data):
        new_node = Node(data)

        if not self.head:
            self.head = new_node
            return

        last_node = self.get_tail()
        new_node.prev = last_node
        last_node.next = new_node

    def insert(self, data, index):
        if self.is_empty():
            self.append(data)
            return


        new_node = Node(data)

        if index == 0:
            self.head.prev = new_node
            new_node.next = self.head
            self.head = new_node
            return

        i = 0
        cur_node = self.head

        while i < index - 1 and cur_node.next:
            cur_node = cur_node.next
            i += 1

        new_node.prev = cur_node
        new_node.next = cur_node.next
        cur_node.next = new_node
        
    
    def __str__(self):
        if self.is_empty():
            return ""

        cur_node = self.head
        st = ""
        while cur_node.next:
            st += str(cur_node.data) + "->"
            cur_node = cur_node.next

        st += str(cur_node.data)
        return st



lt = Double_LinkedList()

for ele in random.randint(1, 10, size=5):
    lt.append(ele)

print(lt)