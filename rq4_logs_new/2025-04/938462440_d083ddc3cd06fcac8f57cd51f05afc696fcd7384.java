package strucky.binaryTree1;

import strucky.linkedlist.Node;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class BreadthFirst {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static List<String> breadthFirstValues(NodeBT<String> root) {

        if (root == null) return List.of();

        List<String> values = new ArrayList<>();
        Queue<NodeBT<String>> queue = new ArrayDeque<>();

        queue.add(root);

        while (! queue.isEmpty()) {
            NodeBT<String> node = queue.remove();
            values.add(node.value);

            if (node.left != null) queue.add(node.left);
            if (node.right != null) queue.add(node.right);
        }

        return values;
    }
}