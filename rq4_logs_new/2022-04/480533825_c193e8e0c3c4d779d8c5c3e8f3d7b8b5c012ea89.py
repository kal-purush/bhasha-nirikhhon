class BST:
    def __init__(self, data = None):
        self.data = data
        self.left = None
        self.right = None
    
    def add_child(self, data):
        # no duplicates
        if data == self.data:
            return
        # add into left subtree
        if data < self.data:
            if self.left:
                # if you're not in the leaf node,
                # make a recursive call until you are
                self.add_child(data)
            else:
                self.left = BST(data)
        # add into right subtree
        else:
            if self.right:
                # recursive call
                self.add_child(data)
            else:
                self.right = BST(data)

    def inOrderTraversal(self):
        elements = []
        # visit left tree
        if self.left:
            elements += self.left.inOrderTraversal()
        
        # visit base node
        elements.append(self.data)

        # visit right tree
        if self.right:
            elements += self.right.inOrderTraversal()
    
        return elements
        