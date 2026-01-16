//https://leetcode.com/problems/subtree-of-another-tree/
package algorithms.easy;

import context.TreeNode;

public class SubtreeOfAnotherTree {

    public static void main(String[] args) {
        SubtreeOfAnotherTree subTree = new SubtreeOfAnotherTree();
        System.out.println("Output:\t" + subTree.isSubtree(
                new TreeNode(3, new TreeNode(4, new TreeNode(1), new TreeNode(2)), new TreeNode(5)),
                new TreeNode(4, new TreeNode(1), new TreeNode(2))));
        System.out.println("Output:\t" + subTree.isSubtree(
                new TreeNode(3, new TreeNode(4, new TreeNode(1), new TreeNode(2, new TreeNode(0), null)),
                        new TreeNode(5)), new TreeNode(4, new TreeNode(1), new TreeNode(2))));
    }

    public boolean isSubtree(TreeNode root, TreeNode subRoot) {
        if (root == null) return false;
        return isSame(root, subRoot) || isSubtree(root.left, subRoot) || isSubtree(root.right, subRoot);
    }

    public boolean isSame(TreeNode rootA, TreeNode rootB) {
        if (rootA == null && rootB == null) return true;
        if (rootA != null && rootB != null)
            return rootA.val == rootB.val && isSame(rootA.left, rootB.left) && isSame(rootA.right, rootB.right);
        return false;
    }
}