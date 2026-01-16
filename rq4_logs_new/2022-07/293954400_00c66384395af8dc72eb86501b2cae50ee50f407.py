class Node(value):
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

class BinaryTree(value):
    def __init__(self, root):
        self.root = Node(root)

    def print_tree(self, traverse_type):
        if traverse_type == "reorder":
            return self.preorder_print(tree.root, "")
        else:
            print(f"The traversal type {traverse_type} is not supported")

    def preorder_print(self, start, traverse_list):
        """This goes Root->Left->Right"""
        if start:
            traverse_list += (str(str(start.value) + "->"))
            traverse_list = self.preorder_print(start.left, traverse_list)
            traverse_list = self.preorder_print(start.right, traverse_list)

        return traverse_list


# This builds our tree
tree = BinaryTree(1)
tree.root.left = Node(2)
tree.root.right = Node(3)
tree.root.left.left = Node(4)
tree.root.left.right = Node(5)
tree.root.right.left = Node(6)
tree.root.right.right = Node(7)
tree.root.right.right.right = Node(8)

tree.print_tree("preorder")