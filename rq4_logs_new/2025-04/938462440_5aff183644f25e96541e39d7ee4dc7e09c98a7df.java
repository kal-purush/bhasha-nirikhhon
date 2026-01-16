package strucky.binaryTree1;

import strucky.linkedlist.Node;

public class MaxRootToLeafSum {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static Double maxPathSum(NodeBT<Double> root) {
        if (root == null)  return Double.NEGATIVE_INFINITY;

        if (root.left == null && root.right == null) return root.value;

        double maxChildSum = Math.max(maxPathSum(root.left), maxPathSum(root.right));

        return root.value + maxChildSum;
    }
}