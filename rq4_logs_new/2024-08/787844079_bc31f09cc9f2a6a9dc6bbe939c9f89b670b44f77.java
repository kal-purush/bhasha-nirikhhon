//https://leetcode.com/problems/diameter-of-binary-tree/
package algorithms.easy;

import context.TreeNode;

public class DiameterOfBinaryTree {
    private int diameter = 0;

    public static void main(String[] args) {
        DiameterOfBinaryTree diameter = new DiameterOfBinaryTree();
        System.out.println("Output:\t" + diameter.diameterOfBinaryTree(
                new TreeNode(1, new TreeNode(2, new TreeNode(4), new TreeNode(5)), new TreeNode(3))));
        System.out.println(
                "Output:\t" + diameter.diameterOfBinaryTree(new TreeNode(1, new TreeNode(2), new TreeNode())));
    }

    public int diameterOfBinaryTree(TreeNode root) {
        dfs(root);
        return diameter;
    }

    public int dfs(TreeNode node) {
        if (node == null) return 0;

        int leftLength = dfs(node.left);
        int rightLength = dfs(node.right);
        diameter = Math.max(diameter, leftLength + rightLength);
        return 1 + Math.max(leftLength, rightLength);
    }
}