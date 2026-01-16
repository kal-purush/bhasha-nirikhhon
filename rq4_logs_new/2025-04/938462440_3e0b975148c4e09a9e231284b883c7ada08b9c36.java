package strucky.binaryTree1;

import strucky.linkedlist.Node;

import java.util.Stack;

public class TreeSum {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static int treeSumDepthFirst(NodeBT<Integer> root) {

        if (root == null) return 0;

        Stack<NodeBT<Integer>> stack = new Stack<>();
        stack.push(root);

        int sum = 0;

        while (! stack.isEmpty()) {

            NodeBT<Integer> node = stack.pop();
            sum += node.value;

            if (node.right != null) stack.push(node.right);
            if (node.left != null) stack.push(node.left);
        }

        return sum;
    }

    public static int treeSumDepthFirstRecursive(NodeBT<Integer> root) {
        if (root == null) return 0;
        return root.value + treeSumDepthFirstRecursive(root.left) + treeSumDepthFirstRecursive(root.right);
    }
}