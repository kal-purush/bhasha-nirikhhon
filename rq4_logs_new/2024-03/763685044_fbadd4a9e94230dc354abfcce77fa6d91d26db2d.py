class BinaryTree:
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.left = left
        self.right = right


def to_bool(func):
    """
    convert value of decorated func in bool type only if the func is NOT called recursively
    """
    def wrapper(*args, **kwargs):
        if 'recursive' in kwargs and kwargs['recursive']:
            return func(*args, **kwargs)
        else:
            return bool(func(*args, **kwargs))

    return wrapper


@to_bool
def is_tree_balanced(node, *args, **kwargs):
    """
    return False if tree is balanced
    return int number of max tree`s height if tree is balanced
    """
    if not node:
        return 1

    left_height, right_height = is_tree_balanced(node.left, recursive=True), is_tree_balanced(node.right,
                                                                                              recursive=True)

    if not left_height or not right_height or abs(left_height - right_height) > 1:
        return False
    else:
        return max(left_height, right_height) + 1