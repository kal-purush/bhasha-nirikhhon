from Trees.Binary_tree import *
from stack_and_queue.stack_and_queue import Queue


def breadth_first(tree):
    if tree is None:
        return []
    if tree.root is None:
        return []

    queue = Queue()
    result = []

    queue.enqueue(tree.root)
    while not queue.is_empty():
        current_node = queue.dequeue()
        result.append(current_node.value)
        if current_node.left is not None:
            queue.enqueue(current_node.left)
        if current_node.right is not None:
            queue.enqueue(current_node.right)
    return result


if __name__ == "__main__":
    tree = BinaryTree()
    tree.root = Node(2)
    tree.root.left = Node(7)
    tree.root.left.left = Node(2)
    tree.root.left.right = Node(6)
    tree.root.left.right.left = Node(5)
    tree.root.left.right.right = Node(11)
    tree.root.right = Node(5)
    tree.root.right.right = Node(9)
    tree.root.right.right.left = Node(4)

    print(breadth_first(tree))
