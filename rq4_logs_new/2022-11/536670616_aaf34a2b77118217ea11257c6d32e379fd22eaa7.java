package Leetcode;

/*
        https://leetcode.com/problems/maximum-depth-of-binary-tree/submissions/
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
    int n = Integer.MIN_VALUE;
    public int maxDepth(TreeNode root) {
        if(root == null){
            return 0;
        } else {
            return helper(root,1);
        }
    }
    public int helper(TreeNode root, int count){
        if(root == null){
            if(count > n){
                n = count;
            }
            return count;
        }
        if(root.left != null){
            helper(root.left, count+1);
        } else {
            helper(root.left, count);
        }
        if(root.right != null){
            helper(root.right, count+1);
        } else {
            helper(root.right, count);
        }
        return n;
    }
}