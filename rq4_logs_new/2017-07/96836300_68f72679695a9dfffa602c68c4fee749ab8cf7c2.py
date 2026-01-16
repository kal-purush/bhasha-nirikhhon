class Stack:
    def __init__(self):
        self.container = []

    def push(self, data):
        self.container.append(data)

    def pop(self):
        return self.container.pop()

    def stackSize(self):
        return len(self.container)

    def isEmpty(self):
        return self.container == []

    def peek(self):
        return self.container[len(self.container) - 1]

    def showStack(self):
        return self.container


class LinkedListNode(object):
    def __init__(self, value):
        self.value = value
        self.next = None