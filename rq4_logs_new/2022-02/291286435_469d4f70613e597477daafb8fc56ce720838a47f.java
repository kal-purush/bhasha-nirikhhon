package com.example.backtracking;

import java.util.*;

/**
 * 路径总和 II
 */
public class Leetcode_113 {

    private class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;
        TreeNode(int x) { val = x; }
    }

    public List<List<Integer>> pathSum(TreeNode root, int targetSum) {
        List<List<Integer>> lists = new ArrayList<>();
        dfs(root, targetSum, new ArrayList<Integer>(), lists);
        return lists;
    }
    private void dfs(TreeNode root, int targetSum, List<Integer> list, List<List<Integer>> lists) {
        if(root == null) {
            return;
        }
        list.add(root.val);
        if(root.left == null && root.right == null) {
            if(root.val == targetSum) {
                lists.add(new ArrayList<>(list));
            }
        } else if(root.left != null && root.right != null) {
            dfs(root.left, targetSum - root.val, list, lists);
            dfs(root.right, targetSum - root.val, list, lists);
        } else if(root.left != null && root.right == null) {
            dfs(root.left, targetSum - root.val, list, lists);
        } else {
            dfs(root.right, targetSum - root.val, list, lists);
        }
        list.remove(list.size() - 1);
    }
}