# Given a binary tree, you need to compute the length of the diameter
# of the tree. The diameter of a binary tree is the length of the
# longest path between any two nodes in a tree. This path may or may not
# pass through the root.

# EXAMPLE:
# Input: Given a binary tree
#           1
#          / \
#         2   3
#        / \
#       4   5
# Output: Return 3, which is the length of the path [4,2,1,3] or [5,2,1,3].

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, x):
#         self.val = x
#         self.left = None
#         self.right = None

class Solution:
    def diameterOfBinaryTree(self, root: TreeNode) -> int:
        '''
        Time: O(M), where M is the number of edges
        Space: O(H), where H is the height of the tree because of recursion
        '''
        # largest length
        self.num_nodes = 0

        self.depth(root)

        return self.num_nodes

    def depth(self, node):
        if not node: return 0
        # calculate depth of a node using dfs
        left = self.depth(node.left)
        right = self.depth(node.right)
        # remember the highest number of nodes used in each path
        self.num_nodes = max(self.num_nodes, left + right)

        return max(left, right) + 1