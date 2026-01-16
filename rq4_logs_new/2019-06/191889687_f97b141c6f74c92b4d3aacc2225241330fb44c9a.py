class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None

def deepcopy(root):
    if root is None:
        return None
    new_root=Node(root)
    new_root.left=deepcopy(root.left)
    new_root.right=deepcopy(root.right)

    return new_root


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

n=deepcopy(root)
print n.left.left.data.data