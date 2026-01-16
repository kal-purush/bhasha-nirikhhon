package com.vmelnyk.geeks4geeks._20240122PathsFromRootWithSpecifiedSum;

import com.vmelnyk.geeks4geeks.Node;

import java.util.ArrayList;

class Solution {
  public static ArrayList<ArrayList<Integer>> printPaths(Node root, int sum) {
    // code here
    final ArrayList<ArrayList<Integer>> result = new ArrayList<>();
    final ArrayList<Integer> path = new ArrayList<>();

    bfs(root, 0, sum, path, result);

    return result;
  }

  private static void bfs(
      final Node node,
      final int sum,
      final int targetSum,
      final ArrayList<Integer> path,
      final ArrayList<ArrayList<Integer>> result) {
    if (node == null) {
      return;
    }
    path.add(node.data);
    if (sum + node.data == targetSum) {
      result.add(new ArrayList<>(path));
    }
    bfs(node.left, sum + node.data, targetSum, new ArrayList<>(path), result);
    bfs(node.right, sum + node.data, targetSum, new ArrayList<>(path), result);
  }
}