# coding: utf-8
# Your code here!
#reverse level order traversal using a stack and bfs
from collections import deque
class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None

def reverseTraversal(root):
    stack=deque()
    reverseStack=deque()
    stack.append(root)
    while stack:
        res=stack.poplefte()
        reverseStack.append(res)
        if res.right:
            stack.append(res.right)
        if res.left:
            stack.append(res.left)
    while reverseStack:
        print reverseStack.pop().data,
'''
root=Node(10)
root.left=Node(7)
root.right=Node(21)
root.left.left=Node(4)
root.left.right=Node(9)
root.left.right.left=Node(8)
root.right.left=Node(15)
root.right.right=Node(22)
root.right.left.right=Node(16)
'''
root=Node(20)
root.left=Node(8)
root.right=Node(22)
root.left.left=Node(5)
root.left.right=Node(30)
root.right.left=Node(4)
root.right.right=Node(25)
root.left.right.left=Node(10)
root.left.right.right=Node(14)

reverseTraversal(root)