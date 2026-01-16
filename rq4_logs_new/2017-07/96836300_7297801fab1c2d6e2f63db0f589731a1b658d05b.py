class Stack:
    def __init__(self):
        self.container = []

    def push(self, data):
        self.container.append(data)

    def pop(self):
        return self.container.pop()

    def isEmpty(self):
        return self.container == []

    def peek(self):
        return self.container[-1]


class MaxStack(Stack):
    def getMax(self):
        currentMax = self.pop()
        while not self.isEmpty():
            currentMax = max(currentMax, self.pop())
        return currentMax


def main():
    stack = MaxStack()
    stack.push(1)
    stack.push(6)
    stack.push(2)
    stack.push(5)
    print stack.getMax()

if __name__ == '__main__':
    main()