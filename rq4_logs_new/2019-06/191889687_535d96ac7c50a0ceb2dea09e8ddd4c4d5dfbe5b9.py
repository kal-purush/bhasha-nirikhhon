class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None
def height(root):
    if root is None:
      return 0
    if root.left is None and root.right is None:
        return 1
    return 1+max(height(root.left),height((root.right)))

def spiralUtil(root,level,flag):
    if root is None:
        return
    if level is 1:
        print root.data,
    if level>1:
        if flag:
            spiralUtil(root.left,level-1, flag)
            spiralUtil(root.right,level-1, flag)
        else:
            spiralUtil(root.right, level - 1, flag)
            spiralUtil(root.left, level - 1, flag)
def printSpiralview(root):
    h=height(root)
    flag=False
    for i in range(1,h+1):
        spiralUtil(root,i,flag)
        flag=not flag
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
print height(root)
printSpiralview(root)