package com.vmelnyk.geeks4geeks._20240119DistributeCandiesInBinaryTree;

import com.vmelnyk.geeks4geeks.Node;

class Solution {

  public static int distributeCandy(Node root) {
    Solution solution = new Solution();
    final int[] traverse = solution.traverse(root);
    return traverse[2];
  }

  private int[] traverse(Node node) {
    if (node == null) {
      return new int[] {0, 0, 0};
    }
    final int[] left = traverse(node.left);
    final int[] right = traverse(node.right);
    final int nodes = left[0] + right[0] + 1;
    final int candies = left[1] + right[1] + node.data;
    final int moves =
        left[2] + right[2] + Math.abs(left[1] - left[0]) + Math.abs(right[1] - right[0]);
    return new int[] {nodes, candies, moves};
  }
}