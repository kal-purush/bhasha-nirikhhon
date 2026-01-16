package rish.leets.grind.easy;

/*
 * Grind 75 : Week 2
 * 
 * Problem #: 110
 * Problem link : https://leetcode.com/problems/balanced-binary-tree/
 * 
 * Date Attempted: 04/07/2023
 * 
 * @author Rishabh Soni
 *
 */
public class BalancedBinaryTree {

    class TreeNode {

        int val;
        TreeNode left;
        TreeNode right;

        TreeNode() {
        }

        TreeNode(int val) {
            this.val = val;
        }

        TreeNode(int val, TreeNode left, TreeNode right) {
            this.val = val;
            this.left = left;
            this.right = right;
        }

    }

    public boolean isBalanced(TreeNode root) {

        if (root == null) {
            return true;
        }

        int left = root.left != null ? getDepth(root.left) : 0;
        int right = root.right != null ? getDepth(root.right) : 0;

        if (Math.abs(right - left) > 1) {
            return false;
        }

        return isBalanced(root.left) && isBalanced(root.right);
    }

    public int getDepth(TreeNode root) {

        if (root == null) {
            return 0;
        }

        int left = root.left != null ? getDepth(root.left) : 0;
        int right = root.right != null ? getDepth(root.right) : 0;

        return Math.max(left, right) + 1;
    }

}