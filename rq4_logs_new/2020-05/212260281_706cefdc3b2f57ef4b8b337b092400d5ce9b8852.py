# DESCRIPTION
# Return the root node of a binary search tree that matches the given preorder traversal.
# It's guaranteed that for the given test cases there is always possible to find a 
# binary search tree with the given requirements.

# EXAMPLE
# Input: [8,5,1,7,10,12]
# Output: [8,5,10,1,7,null,12]

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def bstFromPreorder(self, preorder: List[int]) -> TreeNode:
        '''
        Time: O(N), Must iterate over every element of the list
        Space: O(N), max space needed for recursive stack
        '''
        self.index = 0
        return self.bst_build(preorder, float('inf'))
        
    def bst_build(self, preorder, limit):
        # if we have run out of numbers in list
        # or the curr val is larger than the limit 
        if self.index >= len(preorder) or preorder[self.index] > limit:
            return None
        
        root = TreeNode(preorder[self.index])
        
        # inc to move with the list
        self.index += 1
        
        # must not be larger than the parent value
        root.left = self.bst_build(preorder, root.val)
        # must not be larger than the parents limit
        root.right = self.bst_build(preorder, limit)
        
        return root