from stack_and_queue.stack_and_queue import Queue


class Node:
    """
    Node for KaryTree
    """
    def __init__(self, value):
        self.value = value
        self.children = []


class KaryTree:
    def __init__(self, root=None):
        self.root = root

    def breadth_first(self):
        if self.root is None:
            return 'empty tree'

        queue = Queue()
        version_res = []
        queue.enqueue(self.root)

        while not queue.is_empty():
            node = queue.dequeue()
            version_res.append(node.value)
            for child in node.children:
                queue.enqueue(child)

        return version_res