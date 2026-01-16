//https://leetcode.com/problems/merge-two-binary-trees/
package algorithms.easy;

import context.TreeNode;

public class MergeTwoBinaryTrees {
    public static void main(String[] args) {
        MergeTwoBinaryTrees merge = new MergeTwoBinaryTrees();
        System.out.println(
                "Output:\t" + merge.mergeTrees(new TreeNode(1, new TreeNode(3, new TreeNode(5), null), new TreeNode(2)),
                        new TreeNode(2, new TreeNode(1, null, new TreeNode(4)),
                                new TreeNode(3, null, new TreeNode(7)))));
        System.out.println("Output:\t" + merge.mergeTrees(new TreeNode(1), new TreeNode(1, null, new TreeNode(2))));
    }

    public TreeNode mergeTrees(TreeNode root1, TreeNode root2) {
        if (root1 == null) return root2;
        if (root2 == null) return root1;

        TreeNode merged = new TreeNode(root1.val + root2.val);
        merged.left = mergeTrees(root1.left, root2.left);
        merged.right = mergeTrees(root1.right, root2.right);
        return merged;
    }
}