class TreeNode:
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None

    

class DFS:
    def __init__(self, root):
        self.root = root

    def maxDepthOfBTree(self, node):
        if not node:
            return 0
        
        left = self.maxDepthOfBTree(node.left)
        right = self.maxDepthOfBTree(node.right)

        return 1 + max(left, right)
    
    def pathSum(self, node, sum):
        if not node:
            if sum == 0:
                return True
            else:
                return False
        left = self.pathSum(node.left, sum - node.val)
        right = self.pathSum(node.right, sum - node.val)
        return left or right

    def lowestCommonAncestor(self, root, p, q):
        
        def dfs(node, p, q):
            if node is None:
                return None
        
            # If the current node is one of the nodes we are looking for
            if node == p or node == q:
                return node
            
            # Search for LCA in the left and right subtrees
            left = self.lowestCommonAncestor(node.left, p, q)
            right = self.lowestCommonAncestor(node.right, p, q)
            
            # If p and q are found in left and right subtrees, node is their LCA
            if left and right:
                return node
            
            # If one of the subtrees is None, return the non-None value
            return left if left else right
        return dfs(root, p, q)
        
    def diameterOfBinaryTree(self, root):
        diameter = [0]

        def depth(node):
            if not node:
                return 0
            left = depth(node.left)
            right = depth(node.right)
            diameter[0] = max(diameter[0], left + right)
            return 1 + max(left, right)

        depth(root)
        return diameter[0]