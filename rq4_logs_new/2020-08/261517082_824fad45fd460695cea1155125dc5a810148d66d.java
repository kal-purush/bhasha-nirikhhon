/*
        Find the sum of all left leaves in a given binary tree.

        Example:

                    3
                 /   \
                9    20
                    /  \
                   15   7

        There are two left leaves in the binary tree, with values 9 and 15 respectively. Return 24.
*/
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
        public int sumOfLeftLeaves(TreeNode root) {
                int sum = 0;
                if(root != null){
                        if(isLeaf(root.left)){
                                sum += root.left.val;
                        } else {
                                sum += sumOfLeftLeaves(root.left);
                        }
                        
                        sum += sumOfLeftLeaves(root.right);
                }
                return sum;
        }
        
        private boolean isLeaf(TreeNode root){
                if(root == null){
                        return false;
                } else if(root.left == null && root.right == null){
                        return true;
                }
                return false;
        }
}