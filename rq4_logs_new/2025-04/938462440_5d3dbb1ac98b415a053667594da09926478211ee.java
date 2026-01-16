package strucky.binaryTree1;

import java.util.ArrayDeque;
import java.util.Queue;

public class TreeIncludes {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static boolean treeIncludesDepthFirstRecursive(NodeBT<String> root, String target) {

        if (root == null) return false;

        if (root.value == target) return true;

        return treeIncludesDepthFirstRecursive(root.left, target) || treeIncludesDepthFirstRecursive(root.right, target);
    }

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static boolean treeIncludesBreadthFirst(NodeBT<String> root, String target) {
        Queue<NodeBT<String>> queue = new ArrayDeque<>();
        queue.add(root);

        while (! queue.isEmpty()) {

            NodeBT<String> node = queue.remove();

            if (node.value == target) return true;
            if (node.left != null) queue.add(node.left);
            if (node.right != null) queue.add(node.right);
        }

        return false;
    }
}