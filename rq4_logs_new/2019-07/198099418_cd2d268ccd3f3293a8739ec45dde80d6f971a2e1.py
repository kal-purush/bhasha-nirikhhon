class Node:
    '''Initialize the node class.
       Creating an instance of an element and the reference of the next node

       Args:
        item - This will pass the value of the element

       Runtime : Constant time/O(1)
       '''
    def __init__(self,data):
        self.data = data
        self.ref = None

class SLinkedList():
    '''Initializing single linked list class
       
       Runtime: Constant time/O(1)
    '''
    def __init__(self):
        self.start_node = None

    def add_item(self,data):
        '''Inserting a node to the list

           Args:
                data - arguement will add element into the node
           Runtime: Constant time/O(1)
        '''
        newnode = Node(data)
        if self.start_node  is None:
            self.start_node = newnode
        lastnode = self.start_node
        while lastnode.next:
            lastnode = lastnode.next
        lastnode.next = newnode

    def printlist(self):
        '''Printing out the items in linkedlist
           
           Runtime: Linear time/O(n)
        '''
        currentnode = self.start_node
        while currentnode:
            print(currentnode.data)
            currentnode = currentnode.next