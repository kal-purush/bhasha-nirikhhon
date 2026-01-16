# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def insertIntoBST(self, root: Optional[TreeNode], val: int) -> Optional[TreeNode]:
        if not root:
            return TreeNode(val)
        
        stack = [root]
        
        while stack:
            node = stack.pop()
            if node:
                if node.val < val and not node.right:
                    node.right = TreeNode(val)
                    return root
                if node.val > val and not node.left:
                    node.left = TreeNode(val)
                    return root
                if node.val < val: stack.append(node.right)
                if node.val > val: stack.append(node.left)
                    
        return root