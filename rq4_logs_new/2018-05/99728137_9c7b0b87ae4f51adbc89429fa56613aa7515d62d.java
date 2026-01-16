package net.learning.computerscience.datastructures.topic_6_trees.ideserve.p36_convert_a_binary_tree_to_linked_list;

import java.util.Objects;

/**
 * https://www.ideserve.co.in/learn/convert-a-binary-tree-to-doubly-linked-list
 */
public class Solution {

    public static TreeNode getLinkedList(TreeNode root) {
        buildLinkedList(root);
        TreeNode head = getHead(root);
        return head;
    }

    private static TreeNode getHead(TreeNode node) {
        if (node.left == null) return node;
        return getHead(node.left);
    }

    private static void buildLinkedList(TreeNode current) {
        if (current == null) return;
        buildLinkedList(current.left);
        buildLinkedList(current.right);
        current.prev = getPredecessor(current.left);
        if (current.prev != null) current.prev.next = current;
        current.next = getSuccessor(current.right);
        if (current.next != null) current.next.prev = current;
    }

    private static TreeNode getPredecessor(TreeNode treeNode) {
        if (treeNode == null) return null;
        return treeNode.right != null ? getPredecessor(treeNode.right) : treeNode;
    }

    private static TreeNode getSuccessor(TreeNode treeNode) {
        if (treeNode == null) return null;
        return treeNode.left != null ? getPredecessor(treeNode.left) : treeNode;
    }

}

class TreeNode {
    int data;
    TreeNode left;
    TreeNode right;
    TreeNode prev;
    TreeNode next;

    public TreeNode(int data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeNode treeNode = (TreeNode) o;
        return data == treeNode.data &&
                Objects.equals(left, treeNode.left) &&
                Objects.equals(right, treeNode.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, left, right);
    }

    @Override
    public String toString() {
        return "{" +
                "data=" + data +
                "}";
    }
}