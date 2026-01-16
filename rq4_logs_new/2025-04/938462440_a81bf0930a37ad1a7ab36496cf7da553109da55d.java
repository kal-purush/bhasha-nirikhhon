package strucky.binaryTree1;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class DepthFirst {

    //  Time complexity: O(n)
    //  Space complexity: O(n)
    public static List<String> depthFirstValues (NodeBT<String> root) {

        if (root == null) return List.of();

        Stack<NodeBT<String>> stack = new Stack<>();
        List<String> values = new ArrayList<>();

        stack.push(root);

        while (!stack.isEmpty()) {
            NodeBT<String> current = stack.pop();   //  Each iteration's top element
            values.add(current.value);

            if (current.right != null) stack.push(current.right);
            if (current.left != null) stack.push(current.left);
        }

        return values;
    }

    public static List<String> depthFirstValuesRecursive(NodeBT<String> root) {
        //  If tree has no nodes
        if (root == null) return List.of();

        List<String> leftVals = depthFirstValuesRecursive(root.left);   //  Assume you'll get all the left sided nodes of root
        List<String> rightVals = depthFirstValuesRecursive(root.right); //  Assume you'll get all the right sided nodes of root
        List<String> result = new ArrayList<>();

        result.add(root.value);     //  result = root.val + leftVals + rightVals
        result.addAll(leftVals);
        result.addAll(rightVals);

        return result;
    }
}