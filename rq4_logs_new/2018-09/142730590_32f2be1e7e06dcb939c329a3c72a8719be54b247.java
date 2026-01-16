package com.vthakkar;

/* Find lowest common ancestory in a Binary tree with no node to Parent
https://leetcode.com/problems/lowest-common-ancestor-of-a-binary-tree/description/

CTCI 4.8:
First Common Ancestor: Design an algorithm and write code to find the first common ancestor of two nodes in a binary tree.
Avoid storing additional nodes in a data structure. NOTE: This is not necessarily a binary search tree.
*/
public class LowestCommonAncestor {

    public static TreeNode LCA(TreeNode root, TreeNode p, TreeNode q) {
        if (root == null)
            return null;
        if (root == p || root == q) {
            return root;
        }

        TreeNode left = LCA(root.left, p, q);
        TreeNode right = LCA(root.right, p, q);

        if (left != null && right != null) {
            return root;
        }
        if (left == null && right == null) {
            return null;
        }
        return left != null ? left : right;
    }
}