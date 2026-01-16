#!/usr/bin/python3
import unittest
import queue

class Node:
    def __init__(self, data):
        self.left = None
        self.right = None
        self.nextInLevel = None
        self.data = data
    def insert(self, data):
        if self.data:
            if data < self.data:
                if self.left is None:
                    self.left = Node(data)
                else:
                    self.left.insert(data)
            elif data > self.data:
                if self.right is None:
                    self.right = Node(data)
                else:
                    self.right.insert(data)
        else:
            self.data = data
    def findVal(self, val):
        if val < self.data:
            if self.left is None:
                return str(val)+" Not Found"
            return self.left.findval(val)
        elif val > self.data:
            if self.right is None:
                return str(val)+" Not Found"
            return self.right.findval(val)
        else:
            print(str(self.data) + ' is found')
    def PrintTreeInOrder(self):
        if self.left:
            self.left.PrintTreeInOrder()
        if self.nextInLevel:
            print(self.data, self.nextInLevel.data)
        else:
            print(self.data)
        if self.right:
            self.right.PrintTreeInOrder()


    # Uses the concept of BFS to link nodes in the same level of the tree
    def linkLevels(self):
        self.nodesToLink = queue.Queue()
        self.linked = queue.Queue()
        self.nodesToLink.put(self)

        while not self.nodesToLink.empty():
    # Empties the Queue with nodes to link and adds each node to the liked queue
    # From which the nodesToLink queue is filled again, till there is no more nodes 
            self.currentNode = self.nodesToLink.get()
            while True:
                if not self.nodesToLink.empty():
                    self.nextNode = self.nodesToLink.get()
                    self.currentNode.nextInLevel = self.nextNode
                    self.linked.put(self.currentNode)
                    self.currentNode = self.nextNode
                else:
                    self.linked.put(self.currentNode)
                    break
    # Adds the children of each already linked node from the previous level to the
    # nodesToLink queue
            while not self.linked.empty():
                self.currentNode = self.linked.get()
                if self.currentNode.left:
                    self.nodesToLink.put(self.currentNode.left)
                if self.currentNode.right:
                    self.nodesToLink.put(self.currentNode.right)


class MyTest(unittest.TestCase):
    def setUp(self):
        self.root = Node(10)
        self.root.insert(12)
        self.root.insert(11)
        self.root.insert(13)
        self.root.insert(5)
        self.root.insert(3)
        self.root.insert(6)
        self.root.insert(2)
        self.root.insert(4)
        self.root.insert(7)

    def test_print(self):
        self.root.linkLevels()
        self.root.PrintTreeInOrder()
        
if __name__ == "__main__":
    unittest.main()