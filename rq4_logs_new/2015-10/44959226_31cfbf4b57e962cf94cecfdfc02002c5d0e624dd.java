package Problem_A;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;

/**
 * Created by alex on 30.10.15.
 */
public class NodeBST {

    private static BufferedReader reader;

    private Node root;          // root of BST

    private class Node {
        private int key;              // sorted by key
        private Node left, right;     // left and right subtrees

        Node(int key) {
            this.key = key;
        }
    }

    public Node find(int key) {
        Node current = root;
        while(current.key != key) {
            if (key < current.key)
                current = current.left;
            else
                current = current.right;
            if (current == null)
                return null;

        }
        return current;
    }

    public void insert(int findKey) {
        Node node = new Node(findKey);
        if (root == null)
            root = node;
        else {
            Node current = root;
            Node parent;
            while(true) {
                parent = current;
                if (findKey < current.key) {
                    current = current.left;
                    if (current == null) {
                        parent.left = node;
                        return;
                    }
                }
                else {
                    current = current.right;
                    if(current == null) {
                        parent.right = node;
                        return;
                    }
                }
            }
        }
    }

    public boolean delete(int findKey) {
        Node current = root;
        Node parent = root;
        boolean isLeftChild = true;

        while(current.key != findKey) {
            parent = current;
            if (findKey < current.key) {
                isLeftChild = true;
                current = current.left;
            } else {
                isLeftChild = false;
                current = current.right;
            }
            if (current == null)
                return false;
        }

        if (current.left == null && current.right == null) {
            if (current == root)
                root = null;
            else if (isLeftChild)
                parent.left = null;
            else
                parent.right = null;
        }
        else if (current.right == null) {
            if (current == root)
                root = current.left;
            else if (isLeftChild)
                parent.left = current.left;
            else
                parent.right = current.left;
        } else if(current.left == null) {
            if (current == root)
                root = current.right;
            else if (isLeftChild)
                parent.left = current.right;
            else
                parent.right = current.right;
        } else {
            Node predecessor = getPredecessor(current);
            if (current == root)
                root = predecessor;
            else if (isLeftChild)
                parent.left = predecessor;
            else
                parent.right = predecessor;
            predecessor.left = current.left;
        }
        return true;
    }

    private Node getPredecessor(Node delNode) {
        Node predecessorParent = delNode;
        Node predecessor = delNode;
        Node current = delNode.left;
        while(current != null) {
            predecessorParent = predecessor;
            predecessor = current;
            current = current.right;
        }

        if (predecessor != delNode.left) {
            predecessorParent.right = predecessor.left;
            predecessor.left = delNode.left;
            predecessor.right = delNode.right;
        }
        return predecessor;
    }

    public static int[] parseLine() throws Exception {
        String[] lines = reader.readLine().split(" ");
        int[] nums = new int[lines.length];
        for (int i = 0; i < lines.length; i++) {
            nums[i] = Integer.parseInt(lines[i]);
        }
        return nums;
    }

    public Integer getRightChild(int key) {
        Node q = find(key);
        Node right = q == null ? null : q.right;
        if (right == null)
            return null;
        else
            return right.key;
    }

    public static void main(String[] args) throws Exception {
        NodeBST bst = new NodeBST();

        reader = new BufferedReader(new FileReader(new File("bst.in")));
        PrintWriter writer = new PrintWriter(new File("bst.out"));

        int[] nums = parseLine();
        for (int i = 0; i < nums.length; i++) {
            bst.insert(nums[i]);
        }

        nums = parseLine();
        for (int i = 0; i < nums.length; i++) {
            bst.delete(nums[i]);
        }

        nums = parseLine();
        for (int i = 0; i < nums.length; i++) {
            Integer rightChild = bst.getRightChild(nums[i]);
            writer.print(rightChild + " ");
        }
        reader.close();
        writer.close();
    }
}