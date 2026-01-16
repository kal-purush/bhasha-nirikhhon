# Definition for a binary tree node.
# class TreeNode(object):
#     def __init__(self, x):
#         self.val = x
#         self.left = None
#         self.right = None

class Solution(object):
    def __init__(self):
        self.most_deep_row = 0
        self.bottom_left_val = None
    
    def clear_status(self):
        self.most_deep_row = 0
        self.bottom_left_val = None
    
    def findBottomLeftValue(self, root):
        """
        :type root: TreeNode
        :rtype: int
        """
        self.clear_status()
        self.pre_order_traverse(root, 0)  # start depth=0
        return self.bottom_left_val
        
    def pre_order_traverse(self, root, current_depth):
        if root is None:
            return 
        else:
            if current_depth > self.most_deep_row:  # current row 
                # update the msg
                self.most_deep_row = current_depth+1
                self.bottom_left_val = root.val
            self.pre_order_traverse(root.left, current_depth+1)
            self.pre_order_traverse(root.right, current_depth+1)
        