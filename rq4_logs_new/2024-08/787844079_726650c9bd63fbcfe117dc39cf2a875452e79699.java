//https://leetcode.com/problems/binary-tree-tilt/description/
package algorithms.easy;

import context.TreeNode;

public class BinaryTreeTilt {
    int answer = 0;

    public static void main(String[] args) {
        BinaryTreeTilt tilt = new BinaryTreeTilt();
        System.out.println("Output:\t" + tilt.findTilt(new TreeNode(1, new TreeNode(2), new TreeNode(3))));
        System.out.println("Output:\t" + tilt.findTilt(
                new TreeNode(4, new TreeNode(2, new TreeNode(3), new TreeNode(5)),
                        new TreeNode(9, null, new TreeNode(7)))));
        System.out.println("Output:\t" + tilt.findTilt(
                new TreeNode(21, new TreeNode(7, new TreeNode(1, new TreeNode(3), new TreeNode(3)), new TreeNode(1)),
                        new TreeNode(14, new TreeNode(2), new TreeNode(2)))));
    }

    public int findTilt(TreeNode root) {
        dfs(root);
        return answer;
    }

    public int dfs(TreeNode node) {
        if (node == null) return 0;
        int left = dfs(node.left);
        int right = dfs(node.right);
        answer += Math.abs(left - right);
        return node.val + left + right;
    }
}