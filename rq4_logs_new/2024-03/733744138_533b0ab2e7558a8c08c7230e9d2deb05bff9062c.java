package br.dev.ferreiras.challenges.leetCode;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Intuition
 * First thought that came in mind by seeing tree questions are bfs or dfs .
 * Then as we have to check a strictly increasing or decreasing order in levels
 * so breadth first search would be a good approach to start.
 * After that make an approach and implement it.
 * Approach
 * Here's the explanation of the code:
 * 1-Breadth-First Search (BFS):
 * The code uses BFS to traverse the tree level by level.
 * It utilizes a queue to keep track of the nodes at each level.
 * 2- Flag for Alternating Levels: It uses a boolean flag to determine whether
 * the current level should have odd or even values.
 * 3-Conditions Checking:
 * Inside the BFS loop, the code checks the following conditions for each node:
 * 4- If the current level is odd (flag is true), then the node value should be odd,
 * and it should be strictly greater than the previous node's value.
 * 5- If the current level is even (flag is false), then the node value should be even,
 * and it should be strictly less than the previous node's value.
 * 6- If any of these conditions are not met, the function returns false immediately.
 * 7- Updating Queue: The code enqueues the left and right children of each node (if they exist)
 * into the queue for the next level.
 * 8- Updating Previous Value: The variable prev is updated with the value of the current node,
 * ensuring it's used in the next iteration for comparison.
 * 9- Alternating Levels: After processing each level, the flag is toggled to switch between
 * odd and even levels.
 * 10- Returning Result:
 * If the BFS completes without encountering any violation of conditions, the function returns true,
 * indicating that the tree is an Even-Odd Tree.
 */
public class BinaryTreeEvenOdd {
  public boolean isEvenOddTree(TreeNode root) {
    if (root == null) {
      return true;
    }
    Queue<TreeNode> queue = new LinkedList<>();
    boolean flag = true;
    queue.offer(root);
    while (!queue.isEmpty()) {
      int size = queue.size();
      int prev = flag ? Integer.MIN_VALUE : Integer.MAX_VALUE;
      for (int i = 0; i < size; i++) {
        TreeNode node = queue.poll();
        if (flag) {
          assert node != null;
          if (node.value % 2 == 0 || node.value <= prev) {
            return false;
          }
        } else {
          assert node != null;
          if (node.value % 2 != 0 || node.value >= prev) {
            return false;
          }
        }
        if (node.left != null) {
          queue.offer(node.left);
        }
        if (node.right != null) {
          queue.offer(node.right);
        }
        prev = node.value;
      }
      flag = !flag;
    }
    return true;
  }
}