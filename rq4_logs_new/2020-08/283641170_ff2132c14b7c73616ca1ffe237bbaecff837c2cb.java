/**
 * Definition for a binary tree node.
 * public class TreeNode {
 *     int val;
 *     TreeNode left;
 *     TreeNode right;
 *     TreeNode() {}
 *     TreeNode(int val) { this.val = val; }
 *     TreeNode(int val, TreeNode left, TreeNode right) {
 *         this.val = val;
 *         this.left = left;
 *         this.right = right;
 *     }
 * }
 */
class ValidateBST {
    private Stack<TreeNode> stack = new Stack<>();
    public boolean isValidBST(TreeNode root) {
        return helper(root,null,null);
    }
    boolean helper(TreeNode root,Integer upper, Integer lower){
        if(root == null)
            return true;

        int val = root.val;

        if(upper!=null && val>=upper){return false;}
        if(lower!=null&& val<=lower){return false;}

        if(!helper(root.right,upper,val)){return false;}
        if(!helper(root.left,val,lower)){return false;}
        return true;
    }

}

//TC : O(n).
//sc : O(d)
//logic: simple recursion. at each function check the val is with in limit. if yes, send down the upper and lower limits down the line.