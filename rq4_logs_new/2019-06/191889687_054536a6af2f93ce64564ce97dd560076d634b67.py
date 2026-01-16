class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None

def convertToMirror(root):
    if root is not None:
        lchild=convertToMirror(root.left)
        rchild=convertToMirror(root.right)
        root.left,root.right=root.right,root.left

    return

root=Node(20)
root.left=Node(8)
root.right=Node(22)
root.left.left=Node(5)
root.left.right=Node(30)
root.right.left=Node(4)
root.right.right=Node(25)
root.left.right.left=Node(10)
root.left.right.right=Node(14)

convertToMirror(root)
print root.right.right.data