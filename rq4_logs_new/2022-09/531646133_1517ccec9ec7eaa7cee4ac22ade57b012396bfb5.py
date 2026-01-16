class Stack:
    def __init__(self):
        '''
        Default constructor
        '''

        self.elements = []

    def push(self, element):
        '''
        Push element to the stack
        :param element:
        '''

        self.elements.insert(0,element)

    def pop(self):
        '''
        Pop top element of the stack
        :return: element
        '''

        if not self.isEmpty():
            return self.elements.pop(0)
        else:
            return None

    def peek(self):
        '''
        Peek element at the top of the stack
        :return: element
        '''

        if not self.isEmpty():
            return self.elements[0]
        else:
            return None

    def isEmpty(self):
        '''
        Check if stack is empty
        :return: true if empty, false otherwise
        '''

        if len(self.elements) == 0:
            return True
        else:
            return False

    def size(self):
        '''
        Size of the stack
        :return: number of elements in the stack
        '''
        return len(self.elements)

    def tolist(self):
        '''
        Returns stack as a list
        :return: list
        '''

        return self.elements