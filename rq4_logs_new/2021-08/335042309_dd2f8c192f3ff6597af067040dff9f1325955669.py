class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def max_path_sum(root):

    if root == None:
        return 0 
    
    lmax = max_path_sum(root.left)
    rmax = max_path_sum(root.left)

    max_single = max(max(lmax, rmax) + root.val, root.val)

    max_sub = lmax + root.val + rmax

    max_path_sum.max_sum = max(max(max_single,max_sub), max_path_sum.max_sum)

    return max_single

root = TreeNode(1)
root.left = TreeNode(2)
root.right = TreeNode(3)

max_path_sum(root)
print(max_path_sum.max_path)

root = TreeNode(-10)
root.left = TreeNode(9)
root.right = TreeNode(20)
root.right.left = TreeNode(15)
root.right.right = TreeNode(7)

max_path_sum(root)