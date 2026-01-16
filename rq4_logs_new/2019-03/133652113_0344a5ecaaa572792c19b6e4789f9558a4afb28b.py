"""
Python: Binary Tree
        Optimized Level Order Traversal
        Complexity: O(n)
"""


class Node:
    def __init__(self, data):
        self.data = data
        self.left = None
        self.right = None


# Optimized Level Order Traversal
def optimized_level_traversal(root):
    if root is None:
        return root

    queue = [root]

    while queue.__len__() != 0:
        node = queue.pop(0)
        print(node.data)

        if node.left:
            queue.append(node.left)

        if node.right:
            queue.append(node.right)


if __name__ == "__main__":
    root = Node(1)
    root.left = Node(2)
    root.right = Node(3)
    root.left.right = Node(4)
    root.left.left = Node(5)
    root.right.left = Node(6)
    root.right.right = Node(7)
    root.right.left.left = Node(8)

    optimized_level_traversal(root)