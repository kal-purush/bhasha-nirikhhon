import sys
INT_MAX=sys.maxint
INT_MIN=-1*sys.maxint

class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None
def isBST(root,min,max):
    if root is None:
        return True
    if root.data<min or root.data>max:
        return False
    return isBST(root.left,min,root.data) and isBST(root.right,root.data,max)

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

print isBST(root,INT_MIN,INT_MAX)