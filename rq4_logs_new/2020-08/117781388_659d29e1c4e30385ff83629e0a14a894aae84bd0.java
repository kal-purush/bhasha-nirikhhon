package BinaryTree;

import java.util.HashMap;
import java.util.Map;

public class PathSumIII {
    int count = 0;
    int target = 0;
    Map<Integer, Integer> map = new HashMap<>();

    public int pathSum(TreeNode root, int sum) {
        target = sum;
        preorder(root, 0);
        return count;
    }

    public void preorder(TreeNode node, int currSum) {
        if (node == null) {
            return;
        }

        // current prefix sum
        currSum += node.val;

        // here is the sum we're looking for
        if (currSum == target) {
            count++;
        }

        // number of times the currSum - target has occurred already,
        // determines the number of times a path with sum target
        // has occurred upto the current node.
        count += map.getOrDefault(currSum - target, 0);

        // add the current sum into hashmap to
        // use it during the child nodes processing.
        map.put(currSum, map.getOrDefault(currSum, 0) + 1);

        // process left subtree
        preorder(node.left, currSum);
        // process right subtree
        preorder(node.right, currSum);

        // remove the current sum from the hashmap
        // in order not to use it during the parallel subtree processing
        map.put(currSum, map.get(currSum) - 1);
    }
}