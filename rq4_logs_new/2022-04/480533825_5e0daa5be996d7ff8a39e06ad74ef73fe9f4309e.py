class Node(object):
    def __init__(self, data = None, next = None):
        self.data = data
        self.next = next

class SLinkedList(object):
    def __init__(self):
        self.head = None
        # self.tail = None
    
    def printLinkedList(self):
        if self.head is None:
            print("my linked list is empty")
            return
        llstr = '' # string to append data to.
        itr = self.head
        while itr:
            llstr += str(itr.data) + '-->'
            itr= itr.next
        print(llstr)
    
    # insert in the beginning
    def insert(self, newNode):
       node = Node(newNode, self.head)
       self.head = node
    
    def insert_at_index(self, idx, newNode):
        # base case: check if idx is invalid
        if idx < 0 or idx > self.lengthOfLL():
            raise Exception("not a valid index.")
        # base case: insert root index
        if idx == 0:
            self.insert(newNode)
            return
        # inserting at spefic index
        count = 0
        itr = self.head
        while itr:
            if count == idx - 1:
                node = Node(newNode, itr.next)
                itr.next = node
                break
            itr = itr.next
            count += 1
    
    # insert at end
    def append(self, newNode):
        # if the linked list was empty
        if self.head == None:
            self.head = Node(newNode, None)
            return
        # if the linked list was not empty
        itr = self.head
        # if itr.next hhas some value, than we are not at the end
        while itr.next:
            itr = itr.next
        
        itr.next = Node(newNode, None)
    
    def lengthOfLL(self):
        count = 0
        itr = self.head
        while itr.next:
            count += 1
            itr = itr.next
        return(count)
    
    def remove_at_index(self, idx):
        # base case 1: check if index is valid:
        if idx < 0 or idx > self.lengthOfLL():
            raise Exception("not a valid index")
        # base case 2: attempting to remove the head node
        if idx ==  0:
            self.head = self.head.next
            return
        count = 0
        itr = self.head
        while itr:
            if count == idx - 1:
                itr.next = itr.next.next
                break
            itr = itr.next
            count += 1


if __name__ == '__main__':
    ll = SLinkedList()
    ll.insert(5)
    ll.insert(98)
    ll.append(79)
    ll.append(9876)
    ll.append(345)
    ll.remove_at_index(0)
    ll.insert_at_index(3, 589)
    ll.insert_at_index(3, 12)
    ll.insert_at_index(0, 21)
    # ll.remove_at_index(20) # throws an exception
    # ll.remove_at_index(-1) # throws an exception
    print("length: ", ll.lengthOfLL())
    ll.printLinkedList()