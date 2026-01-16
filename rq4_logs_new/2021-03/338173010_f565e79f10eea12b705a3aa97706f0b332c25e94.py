# Leetcode easy

# Given the root of a binary tree, 
# check whether it is a mirror of itself (i.e., symmetric around its center).

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right
    
class Solution:
    def isSymmetric(self, root: TreeNode) -> bool:
        if root is None:
            return True
        elif root.left is None or root.right is None or root.left.val != root.right.val:
            return False
        elif root.left.left is None or root.right.right is None or root.left.left.val != root.right.right.val:
            return False
        elif root.left.right is None or root.right.left is None or root.left.right.val != root.right.left.val:
            return False
        else:
            return True
        