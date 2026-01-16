/*
 * @lc app=leetcode id=105 lang=java
 *
 * [105] Construct Binary Tree from Preorder and Inorder Traversal
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
    public TreeNode buildTree(int[] preorder, int[] inorder) {
        if (preorder == null || inorder == null || preorder.length != inorder.length) {
            return null;
        }
        Map<Integer, Integer> inorderIndex = new HashMap();
        for (int i = 0; i < inorder.length; i++) {
            inorderIndex.put(inorder[i], i);
        }
        
        return buildMyTree(preorder, 0, preorder.length, inorder, 0, inorder.length, inorderIndex);
    }

    private TreeNode buildMyTree(int[] preorder, int preLeft, int preRight, int[] inorder, int inLeft, int inRight, Map<Integer, Integer> inorderIndex) {
        if (preLeft == preRight) {
            return null;
        }
        TreeNode root = new TreeNode(preorder[preLeft]);
        int index = inorderIndex.get(root.val);
        root.left = buildMyTree(preorder, preLeft + 1, preLeft + 1 + index - inLeft, inorder, inLeft, index, inorderIndex);
        root.right = buildMyTree(preorder, preLeft + 1 + index - inLeft, preRight, inorder, index + 1, inRight, inorderIndex);
        return root;
    }
}
// @lc code=end
