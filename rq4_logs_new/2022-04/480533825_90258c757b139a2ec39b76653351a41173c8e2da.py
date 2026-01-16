'''
Goal: Implement a binary search tree
Binary Search trees should have:
- Binary Tree
- Nodes
- children nodes

functions:
- get Node data
- add node
- delete node
- search for node on binary tree
'''
from numpy import left_shift


class Node(object):
    # Node has a key and data.
    # access the node's data with the node's key
    def __init__(self, node, data, key):
        self.node = node
        self.data = data
        self.key = key
        node[key] = data


class BinaryTree(object):
    def __init__(self, left, right ):
        self.node = Node.node
        self.left = left
        self.right = right
    
    def __getnode__(self):
        return Node.data
    

