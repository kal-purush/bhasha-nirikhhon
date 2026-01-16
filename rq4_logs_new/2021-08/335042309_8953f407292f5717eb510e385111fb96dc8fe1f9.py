class Tree:
    def __init__(self, val, left=None, right=None):
        self.val = val 
        self.left = left 
        self.right = right 

class Solution:

    def __init__(self):
        self.maxpath = 0

    def diameter(self, root):

        if root==None:
            return 0 

        lmax = self.diameter(root.left)
        rmax = self.diameter(root.right)

        self.maxpath = max(lmax + rmax, self.maxpath)
        
        return max(lmax, rmax)+1

    def diameterOfBinaryTree(self, root) -> int:
        
        return self.diameter(root)


