package strucky.binaryTree1;

import java.util.ArrayDeque;
import java.util.Queue;

public class MinValue {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static Double treeMinValueDepthFirstRecursive(NodeBT<Double> root) {

        if (root == null) return Double.POSITIVE_INFINITY;

        double minChildVal = Math.min(treeMinValueDepthFirstRecursive(root.left), treeMinValueDepthFirstRecursive(root.right));

        return Math.min(root.value, minChildVal);
    }

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static Double treeMinValueBreadthFirst(NodeBT<Double> root) {
        Queue<NodeBT<Double>> queue = new ArrayDeque<>();
        queue.add(root);

        double minChildVal = 0;

        while (! queue.isEmpty()) {
            NodeBT<Double> node = queue.remove();

            minChildVal = Math.min(minChildVal, node.value);

            if (node.left != null) queue.add(node.left);
            if (node.right != null) queue.add(node.right);
        }

        return Math.min(root.value, minChildVal);
    }
}