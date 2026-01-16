class SimpleTrieNode(object):
    def __init__(self):
        self._children = {}
        self._prefix = True

    def get_child_at_char(self, c):
        return self._children.get(c, None)

    def add_child_at_char(self, c):
        node = SimpleTrieNode()
        self._children[c] = node
        return node

    def is_leaf(self):
        return len(self._children) == 0

    def is_prefix(self):
        return self._prefix

    def set_prefix(self, prefix):
        self._prefix = prefix


class SimpleTrie(object):
    def __init__(self):
        self._root = SimpleTrieNode()

    def add_word(self, word):
        curr_node = self._root

        for char in word:
            child = curr_node.get_child_at_char(char)
            if not child:
                child = curr_node.add_child_at_char(char)
            curr_node = child

        curr_node.set_prefix(False)

    '''
    Returns True if word is found, False if only a prefix,
    else None.
    '''

    def find_word(self, word):
        curr_node = self._root

        for char in word:
            child = curr_node.get_child_at_char(char)
            if not child:
                return None
            curr_node = child

        return curr_node.is_prefix()