# coding: utf-8
# Your code here!
from collections import deque

#initialise node 
class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None
        self.hd=None
def findLCA(root,p,q):
    if root==None:
        return None
    if root.data==p or root.data==q:
        return root
    left=findLCA(root.left,p,q)
    right=findLCA(root.right,p,q)
    
    if left!=None and right!=None:
        return root
    else:
        return left if left is not None else right
root=Node(20)
root.left=Node(8)
root.right=Node(22)
root.left.left=Node(5)
root.left.right=Node(30)
root.right.left=Node(4)
root.right.right=Node(25)
root.left.right.left=Node(10)
root.left.right.right=Node(14)
print "LCA is : "
print findLCA(root,3,8).data