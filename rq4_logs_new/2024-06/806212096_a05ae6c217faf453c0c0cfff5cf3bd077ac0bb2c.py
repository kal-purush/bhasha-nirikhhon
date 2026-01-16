class TreeNode:
    def __init__(self, data):
        self.data = data
        self.left = None
        self.right = None

    def __str__(self):
        return str({"node": self.data})


class BinaryTree:
    def __init__(self, root=None):
        self.root = root

    
    def inOrderTraversal(self, node):
        if node is None:
            return
        self.inOrderTraversal(node.left)
        print(node.data, end=", ")
        self.inOrderTraversal(node.right)

    
    def preOrderTraversal(self, node):
        if node is None:
            return

        print(node.data, end=", ")
        self.preOrderTraversal(node.left)
        self.preOrderTraversal(node.right)

    def postOrderTraversal(self, node):
        if node is None:
            return

        self.postOrderTraversal(node.left)
        self.postOrderTraversal(node.right)
        print(node.data, end=", ")


    def search(self, target):
        node = self.root

        while node:
            if node.data == target:
                return node
            elif node.data > target:
                node = node.left
            else:
                node = node.right



    def insert(self, value):
        if self.root is None:
            self.root = TreeNode(value)

        node = self.root

        while node:
            if node.data > value:
                
                if node.left is None:
                    node.left = TreeNode(value)
                    break
                else:
                    node = node.left
                    
            elif node.data <= value:
                if node.right is None:
                    node.right = TreeNode(value)
                    break
                else:
                    node = node.right


    def minValue(self):
        current = self.root
        
        while current.left is not None:
            current = current.left
        
        return current

    def maxValue(self):
        current = self.root
        
        while current.right is not None:
            current = current.right
            
        return current

# Setup for Binary Tree
root = TreeNode(13)
node7 = TreeNode(7)
node15 = TreeNode(15)
node3 = TreeNode(3)
node8 = TreeNode(8)
node14 = TreeNode(14)
node19 = TreeNode(19)
node18 = TreeNode(18)

root.left = node7
root.right = node15

node7.left = node3
node7.right = node8

node15.left = node14
node15.right = node19

node19.left = node18


# Traverse
tree = BinaryTree(root)
print("In Order Traversal : " , end="")
tree.inOrderTraversal(root)
print()

print("pre Order Traversal : " , end="")
tree.preOrderTraversal(root)
print()

print("post Order Traversal : " , end="")
tree.postOrderTraversal(root)
print()

# Inserting new element
tree.insert(20)

print("pre Order Traversal : " , end="")
tree.preOrderTraversal(root)
print()

# Searching in Binary tree
print("Searched Node", tree.search(target=20))


# Finding Min and max value in binary tree

print("Min Value", tree.minValue())
print("Max Value", tree.maxValue())