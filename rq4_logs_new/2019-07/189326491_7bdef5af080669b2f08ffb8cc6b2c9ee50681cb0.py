class Node(object):
    def __init__(self, data):
        self.data = data
        self.previousNode= None
        self.nextNode = None


class DoublyLinkedList(object):
    def __init__(self):
        self.headNode = None
        self.tailNode = None

    def insertBeofreHeadNode(self, nodeToInsert):
        if self.headNode is None:
            newNode = Node(nodeToInsert)
            self.headNode = newNode
            self.tailNode = Node(None)
            self.tailNode.previousNode = self.headNode
        else:
            newNode = Node(nodeToInsert)
            tempNode = self.headNode
            self.headNode = newNode
            tempNode.previousNode = self.headNode
            self.headNode.nextNode = tempNode

    def insertAfterHead(self, nodeToInsert):
        if self.headNode is None:
            newNode = Node(nodeToInsert)
            self.headNode = newNode
            self.tailNode = Node(None)
            self.tailNode.previousNode = self.headNode
        else:
            newNode = Node(nodeToInsert)
            newNode.previousNode = self.headNode
            tempNode = self.headNode.nextNode
            tempNode.previousNode = newNode
            self.headNode.nextNode = newNode
            newNode.nextNode = tempNode

    def printTheList(self):
        children = []
        currentNode = self.headNode

        while currentNode:
            children.append(currentNode.data)
            currentNode = currentNode.nextNode
        return children

    def peekHead(self):
        return self.headNode.data

    def peekTail(self):
        return self.tailNode.previousNode.data


DLL = DoublyLinkedList()
array = [2, 4, 7, 8, 9, 10]
for x in array:
    DLL.insertBeofreHeadNode(x)

array2 = [34, 54, 66, 88]
for x in array2:
    DLL.insertAfterHead(x)

print(DLL.printTheList())
print(DLL.peekHead())
print(DLL.peekTail())
