from Trees.Binary_tree import Node, BinaryTree


class BinarySearchTree(BinaryTree):
    """
    this class is a  subclass of the BinaryTree class with Add and Contain methods
    """
    def __init__(self):
        super().__init__()
        self.root = None

    def add(self, value):
        if self.root is None:
            self.root = Node(value)
            return
        cur_node = self.root
        added_to_tree = False
        while not added_to_tree:
            if cur_node.value >= value:
                if cur_node.left is None:
                    cur_node.left = Node(value)
                    added_to_tree = True
                else:
                    cur_node = cur_node.left
            elif cur_node.value < value:
                if cur_node.right is None:
                    cur_node.right = Node(value)
                    added_to_tree = True
                else:
                    cur_node = cur_node.right

    def contains(self, value):
        bool_res = False
        cur_node = self.root
        while not bool_res:
            if cur_node.value == value:
                return True
            elif cur_node.value > value:
                if cur_node.left is None:
                    bool_res = True
                else:
                    cur_node = cur_node.left
            elif cur_node.value < value:
                if cur_node.right is None:
                    bool_res = True
                else:
                    cur_node = cur_node.right
        return False


if __name__ == '__main__':
    t = BinarySearchTree()
    t.add(10)
    t.add(5)
    t.add(8)
    t.add(3)
    t.add(20)
    t.add(25)
    t.add(15)
    print(t.contains(3))  # True
    print(t.contains(100))  # False
