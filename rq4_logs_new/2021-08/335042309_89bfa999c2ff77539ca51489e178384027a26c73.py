class Tree:
    def __init__(self, val, left=None, right=None):
        self.val = val 
        self.left = left 
        self.right = right 

def is_balanced(root): # We return if BT is balanced or not, along with height of current node

    if root==None:
        return True, 0 

    is_bal_left, l_ht = is_balanced(root.left)

    if not is_bal_left: 
        return False, 0
    
    is_bal_right, r_ht = is_balanced(root.right)

    if not is_bal_right:
        return False, 0 

    return is_bal_left and is_bal_right and abs(l_ht - r_ht)<=1, max(l_ht, r_ht)+1
