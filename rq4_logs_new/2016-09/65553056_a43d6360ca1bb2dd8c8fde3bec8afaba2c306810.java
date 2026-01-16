package default1;

import java.util.Iterator;
import java.util.Stack;

/**
 * Created by Shiyi on 9/23/2016.
 * Based on code from algs4.cs.princeton.edu
 */
public class BST2<Key extends Comparable<Key>, Value> implements Iterable<Value> {
    @Override
    public Iterator<Value> iterator() {
        return new BST2Iterator(root);
    }
    private class BST2Iterator implements Iterator<Value> {
        private Stack<Node> st;
        private BST2Iterator(Node root) {
            st = new Stack<>();
            while(root!=null) {
                st.push(root);
                root = root.left;
            }
        }
        @Override
        public boolean hasNext() {
            return !st.isEmpty();
        }

        @Override
        public Value next() {
            Node node = st.pop();
            Value val = node.val;
            node = node.right;
            while(node!=null) {
                st.push(node);
                node = node.left;
            }
            return val;
        }
    }

    private class Node {
        Key key;
        Value val;
        Node left;
        Node right;

        Node(Key key, Value val) {
            this.key = key;
            this.val = val;
            this.left = null;
            this.right = null;
        }
    }

    private Node root;
    private Node tempNode;
    public BST2() {
        root = null; // can be omitted
        tempNode = null; // can be omitted
    }
    public void put(Key key, Value val) {
        root = put(root, key, val);
    }
    private Node put(Node node, Key key, Value val) {
        if (node == null) node = new Node(key, val);
        else {
            int comp = key.compareTo(node.key);
            if (comp < 0)
                node.left = put(node.left, key, val);
            else if (comp > 0)
                node.right = put(node.right, key, val);
            else // comp == 0, then update the value
                node.val = val;
        }
        return node;
    }
    public Value get(Key key) {
        Node current = root;
        while(current!=null) {
            int comp = key.compareTo(current.key);
            if (comp < 0) current = current.left;
            else if (comp > 0) current = current.right;
            else return current.val;
        }
        return null;
    }
    public void delete(Key key) {
        root = delete(root, key);
    }
    private Node delete(Node node, Key key) {
        //if (node == null) throw new IllegalArgumentException("key " + key + " doesn't exist");
        if (node == null) return null; // doesn't change anything. (no delete).
                                        // because it's null, and return null to whatever points to it. Nothing changed
        int comp = key.compareTo(node.key);
        if (comp < 0) node.left = delete(node.left, key);
        else if (comp > 0) node.right = delete(node.right, key);
        else { // (comp == 0)
            if (node.left == null) return node.right;
            if (node.right == null) return node.left;
            /*if (node.left == null && node.right == null)
                return null;
            else if (node.left == null && node.right != null)
                return node.right;
            else if (node.left != null && node.right == null)
                return node.left;
            else { //node.left != null && node.right != null
            */
                node.right = extractMin(node.right);
                Node rightTreeMin = tempNode;
                rightTreeMin.left = node.left;
                rightTreeMin.right = node.right;
                return rightTreeMin;
            //}
        }
        return node;
    }
    private Node extractMin(Node node) {
        if (node.left == null) { //min found
            tempNode = node; // a global variable
            if (node.right != null) return node.right;
            else return null;
        }
        else { //if (node.left != null)
            node.left = extractMin(node.left);
            return node;
        }
    }

    public void treeTraversal(int type) {
        switch (type) {
            case 1:
                preOrderTraversal(root);
                break;
            case 2:
                inOrderTraversal(root);
                break;
            case 3:
                postOrderTraversal(root);
                break;
            default:
                throw new IllegalArgumentException("valid types are: 1(pre order) 2(in order) 3(post order)");
        }
    }
    private void preOrderTraversal(Node node) {
        System.out.println(node.key + " " +  node.val);
        if (node.left!=null)
            preOrderTraversal(node.left);
        if (node.right!=null)
            preOrderTraversal(node.right);
    }
    private void inOrderTraversal(Node node) {
        if (node.left!=null)
            inOrderTraversal(node.left);
        System.out.println(node.key + " " +  node.val);
        if (node.right!=null)
            inOrderTraversal(node.right);
    }
    private void postOrderTraversal(Node node) {
        if (node.left!=null)
            postOrderTraversal(node.left);
        if (node.right!=null)
            postOrderTraversal(node.right);
        System.out.println(node.key + " " +  node.val);
    }

}