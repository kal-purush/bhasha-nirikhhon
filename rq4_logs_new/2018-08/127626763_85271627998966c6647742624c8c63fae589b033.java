package problems.p199;

import common.TreeNode;

import java.util.LinkedList;
import java.util.List;

/**
 * Definition for a binary tree node.
 * public class TreeNode {
 * int val;
 * TreeNode left;
 * TreeNode right;
 * TreeNode(int x) { val = x; }
 * }
 */
class Solution {
    public List<Integer> rightSideView(TreeNode root) {
        List<Integer> nodes = new LinkedList<>();
        travel(root, nodes, 1);
        return nodes;
    }

    private void travel(TreeNode root, List<Integer> nodes, int level) {
        if (root == null) return;
        if (level > nodes.size()) {
            nodes.add(root.val);
        }
        travel(root.right, nodes, level + 1);
        travel(root.left, nodes, level + 1);
    }
}