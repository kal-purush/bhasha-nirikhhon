# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right

class Solution:

    def maxDepth(self, root: Optional[TreeNode]) -> int:

        Q, max_dep = [], 0
        Q.append ((root, 1))

        while Q:
            node, dep = Q.pop ()
            if node is None: continue
            max_dep = max (dep, max_dep)
            Q.append ((node.left , dep + 1))
            Q.append ((node.right, dep + 1))

        return max_dep