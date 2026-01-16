package dataStructures.tree;

public class FizzBuzzTree {
    public static KaryNode fizzBuzzTree(KaryNode root) {
        return traverse(root);
    }

    private static String fizzBuzz(int value) {
        if (value % 3 == 0 && value % 5 == 0) {
            return "FizzBuzz";
        } else if (value % 3 == 0) {
            return "Fizz";
        } else if (value % 5 == 0) {
            return "Buzz";
        } else {
            return Integer.toString(value);
        }
    }

    private static KaryNode traverse(KaryNode node) {
        if (node == null) {
            return null;
        }

        KaryNode newNode = new KaryNode(fizzBuzz(Integer.parseInt(node.value)));
        for (KaryNode child : node.childrenNodes) {
            newNode.childrenNodes.add(traverse(child));
        }

        return newNode;
    }

    public static void printTree(KaryNode root) {
        if (root != null) {
            System.out.print(root.value + " -> [");
            for (KaryNode child : root.childrenNodes) {
                printTree(child);
            }
            System.out.println("]");
        }
    }
}
