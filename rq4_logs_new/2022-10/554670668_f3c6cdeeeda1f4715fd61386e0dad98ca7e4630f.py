# Write here the code challenge solution
class Node:
    def __init__(self,vlaue):
        self.next = None
        self.value = vlaue

class Stack:
    def __init__(self):
        self.top = None
        self.size = 0

    def push(self,value):
        '''pushes a new node to the top of the stack'''
        node = Node(value)
        if self.top:
            node.next = self.top
        self.top = node
        self.size += 1

    def pop(self):
        '''removes the node from the top of the stack, and returns the node’s value'''
        if self.top:
            temp = self.top
            self.top = self.top.next
            self.size -= 1
            return temp.value
        else:
            return("This stack is empty")
    
    def peek(self):
        '''returns the value of the node located on top of the stack, without removing it from the stack'''
        if self.top:
            return self.top.value
        else:
            return("This stack is empty")

    def empty(self):
        '''returns a boolean indicating whether or not the stack is empty'''
        return self.size == 0



class myQueue:
    def __init__(self):
        self.stack1 = Stack()
        self.stack2 = Stack()
        
    def push(self,value):
        '''pushes a new node to the first position of the queue'''
        while self.stack2.top:
            self.stack1.push(self.stack2.pop())

        self.stack1.push(value)

        while self.stack1.top:
            self.stack2.push(self.stack1.pop())

    def pop(self):
        '''removes the node from the first position of the queue, and returns the node’s value'''
        return self.stack2.pop()
        
    def peek(self):
        '''returns the value of the node located in the first position of the queue, without removing it from the queue'''
        return self.stack2.peek()

    def empty(self):
        '''returns a boolean indicating whether or not the queue is empty'''
        return self.stack2.empty()


if __name__ == "__main__":
    myQueue = myQueue()

    myQueue.push(1)
    myQueue.push(2)
    print(myQueue.peek())
    print(myQueue.pop())
    print(myQueue.peek())
    print(myQueue.empty())