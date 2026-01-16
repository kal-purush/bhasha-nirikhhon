//https://leetcode.com/problems/find-mode-in-binary-search-tree/
package algorithms.easy;

import context.TreeNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FindModeInBinarySearchTree {
    Map<Integer, Integer> map = new HashMap<>();
    int maxFreq = 0;

    public static void main(String[] args) {
        FindModeInBinarySearchTree find = new FindModeInBinarySearchTree();
        System.out.println("Output:\t" + find.findMode(
                new TreeNode(1, new TreeNode(), new TreeNode(2, new TreeNode(), new TreeNode(2)))));
        System.out.println("Output:\t" + find.findMode(new TreeNode(0)));
    }

    public int[] findMode(TreeNode root) {
        preOrderTraversal(root);
        List<Integer> list = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : map.entrySet())
            if (entry.getValue() >= maxFreq) list.add(entry.getKey());

        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    public void preOrderTraversal(TreeNode node) {
        if (node == null) return;
        map.put(node.val, map.getOrDefault(node.val, 0) + 1);
        maxFreq = Math.max(maxFreq, map.get(node.val));
        preOrderTraversal(node.left);
        preOrderTraversal(node.right);
    }
}