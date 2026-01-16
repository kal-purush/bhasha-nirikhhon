package problem4.binaryST;

import problem1.node.TreeNode;
import problem4.myqueue.MyQueue;

public class MyBinarySearchTree {
    private MyQueue myQueue;
    private TreeNode root;


    public MyBinarySearchTree(MyQueue queue) {
        this.myQueue = queue;
    }

    public void insert(int data) {
        TreeNode node = new TreeNode(data);
        if (root == null) {
            root = node;
        } else {
            TreeNode current = root;
            TreeNode parent = null;
            while (current != null) {
                parent = current;
                if (node.getData() < current.getData()) {
                    current = current.getLeft();
                } else {
                    current = current.getRight();
                }
            }
            if (node.getData() < parent.getData()) {
                parent.setLeft(node);
            } else {
                parent.setRight(node);
            }
        }
    }

    public TreeNode getRoot() {
        return root;
    }

    public void setRoot(TreeNode root) {
        this.root = root;
    }

    public void preOrder(TreeNode temp) {
        if (temp == null) {
            return;
        } else {
//            System.out.println(temp.getData());
            myQueue.enqueue(temp.getData());
            preOrder(temp.getLeft());
            preOrder(temp.getRight());
        }
    }

    public void inOrder(TreeNode temp) {
        if (temp == null) {
            return;
        } else {

            inOrder(temp.getLeft());
            System.out.println(temp.getData());
            inOrder(temp.getRight());
        }
    }
}