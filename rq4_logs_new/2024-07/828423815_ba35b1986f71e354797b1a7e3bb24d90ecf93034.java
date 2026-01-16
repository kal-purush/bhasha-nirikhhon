/*
 * @lc app=leetcode id=145 lang=java
 *
 * [145] Binary Tree Postorder Traversal
 */

// @lc code=start


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
class Solution {
    public List<Integer> postorderTraversal(TreeNode root) {

        List<Integer> res = new ArrayList<>();
        Stack<TreeNode> stack = new Stack<>();
        Stack<TreeNode> output = new Stack<>();

        if(root == null) return res;

        TreeNode currNode = root;

        // TreeNode currNode = root;
        stack.add(root);

        while(!stack.isEmpty() && currNode != null){

            currNode = stack.pop();

            output.push(currNode);

            if(currNode.left != null) {
                stack.add(currNode.left);
            }
            if(currNode.right != null) {
                stack.add(currNode.right);
            }
            

        }

        while(!output.isEmpty()){
            res.add(output.pop().val);
        }

        return res;

        
    }
}
// @lc code=end
