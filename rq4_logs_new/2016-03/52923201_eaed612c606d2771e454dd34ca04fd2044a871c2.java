package com.waffelmonster.expressionparser.binarytree;

import com.waffelmonster.expressionparser.conversion.Expression;
import com.waffelmonster.expressionparser.conversion.Operations;

import java.util.*;

/**
 * Created by jonnyfrey on 3/2/16.
 */
public class BinaryTreeUtils {

    public static Node convertPostfixToTree(String[] args) {
        Stack<Node> stack = new Stack<>();
        for (int pos = 0; pos < args.length; pos++) {
            String token = args[pos];
            if (Expression.isNumeric(token)) {
                stack.push(new Node(token));
                continue;
            }
            if (Operations.isAnOperator(token)) {
                if (stack.size() < 2) {
                    throw new IllegalStateException("Not a valid expression");
                }
                Node val2 = stack.pop();
                Node val1 = stack.pop();
                stack.push(new Node(token).withLeft(val1).withRight(val2));
                continue;
            }
            throw new IllegalArgumentException("Found an illegal character: " + token + " pos: " + pos);
        }
        if (stack.size() != 1) {
            throw new IllegalArgumentException("Expression contains extra values");
        }
        return stack.pop();
    }

    public static String[] convertTreeToInfix(Node root) {
        return collapseNode(root, null).getData().split(" ");
    }

    private static Node collapseNode(Node root, Node parent) {

        if (root == null) {
            return new Node();
        }

        String data = collapseNode(root.getLeft(), root).getData()
                + " " + root.getData()
                + " " + collapseNode(root.getRight(), root).getData();
        data = data.trim();
        if (parent != null && Operations.isAnOperator(root.getData())) {
            if (Operations.getOperator(root.getData()).compareTo(Operations.getOperator(parent.getData())) < 0) {
                data = "( " + data + " )";
            }
        }

        return new Node(data);
    }

    public static void printNode(Node root) {
        int maxLevel = maxLevel(root);

        printNodeInternal(Collections.singletonList(root), 1, maxLevel);
    }

    private static <T extends Comparable<?>> void printNodeInternal(List<Node> nodes, int level, int maxLevel) {
        if (nodes.isEmpty() || isAllElementsNull(nodes))
            return;

        int floor = maxLevel - level;
        int endgeLines = (int) Math.pow(2, (Math.max(floor - 1, 0)));
        int firstSpaces = (int) Math.pow(2, (floor)) - 1;
        int betweenSpaces = (int) Math.pow(2, (floor + 1)) - 1;

        printWhitespaces(firstSpaces);

        List<Node> newNodes = new ArrayList<Node>();
        for (Node node : nodes) {
            if (node != null) {
                System.out.print(node.getData());
                newNodes.add(node.getLeft());
                newNodes.add(node.getRight());
            } else {
                newNodes.add(null);
                newNodes.add(null);
                System.out.print(" ");
            }

            printWhitespaces(betweenSpaces);
        }
        System.out.println("");

        for (int i = 1; i <= endgeLines; i++) {
            for (int j = 0; j < nodes.size(); j++) {
                printWhitespaces(firstSpaces - i);
                if (nodes.get(j) == null) {
                    printWhitespaces(endgeLines + endgeLines + i + 1);
                    continue;
                }

                if (nodes.get(j).getLeft() != null)
                    System.out.print("/");
                else
                    printWhitespaces(1);

                printWhitespaces(i + i - 1);

                if (nodes.get(j).getRight() != null)
                    System.out.print("\\");
                else
                    printWhitespaces(1);

                printWhitespaces(endgeLines + endgeLines - i);
            }

            System.out.println("");
        }

        printNodeInternal(newNodes, level + 1, maxLevel);
    }

    private static void printWhitespaces(int count) {
        for (int i = 0; i < count; i++)
            System.out.print(" ");
    }

    private static <T extends Comparable<?>> int maxLevel(Node node) {
        if (node == null)
            return 0;

        return Math.max(maxLevel(node.getLeft()), maxLevel(node.getRight())) + 1;
    }

    private static <T> boolean isAllElementsNull(List<T> list) {
        for (Object object : list) {
            if (object != null)
                return false;
        }

        return true;
    }

    public static Node generateRandomExpression(int floor, int ceiling, int treeSize) {
        return createTree(new RandomGenerator(floor, ceiling), treeSize);
    }

    private static Node createTree(RandomGenerator generator, double size) {
        if (size <= 1) {
            return new Node(generator.random());
        }
        double newSize = Math.floor(size / 2);

        return new Node(RandomGenerator.getRandomOperator()).withLeft(createTree(generator, newSize)).withRight(createTree(generator, newSize));
    }

    private static class RandomGenerator {
        private final int low;
        private final int high;

        public RandomGenerator(int low, int high) {
            if (low > high) {
                throw new IllegalArgumentException("Low is greater than high");
            }
            this.low = low;
            this.high = high - low + 1;
        }

        public long random() {
            return (long) (Math.random() * high) + low;
        }

        private static String getRandomOperator() {
            List<String> keys = new ArrayList<String>(Operations.getListOfOperators().keySet());
            RandomGenerator randomGenerator = new RandomGenerator(0, keys.size() - 1);
            return keys.get((int)randomGenerator.random());
        }
    }

}