import timeit

#initialise node
class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None
        self.hd=None
def findLeaves(root):
    if root==None:
        return
    if root.left==None and root.right==None:
        print root.data,
    findLeaves(root.left)
    findLeaves(root.right)

root=Node(20)
root.left=Node(8)
root.right=Node(22)
root.left.left=Node(5)
root.left.right=Node(30)
root.right.left=Node(4)
root.right.right=Node(25)
root.left.right.left=Node(10)
root.left.right.right=Node(14)

findLeaves(root)