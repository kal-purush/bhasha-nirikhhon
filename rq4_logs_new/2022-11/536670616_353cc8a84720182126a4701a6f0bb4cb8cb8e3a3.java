package com.company.BinaryTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class InorderBt {

    private TreeNode root;

    public static void main(String[] args) {
        InorderBt bt  = new InorderBt();
        bt.createBinaryTree();
//        bt.inOrderRec(bt.root);
//        bt.inOrderIter(bt.root);
        List<Integer> val = bt.inOrderRecList(bt.root);
        System.out.println(val);
    }

    // iterative in-order traversal
    public void inOrderIter(TreeNode root){
        Stack<TreeNode> stack = new Stack<>();
        TreeNode temp = root;
        while(!stack.isEmpty() || temp != null){
            if(temp != null){
                stack.push(temp);
                temp = temp.left;
            } else {
                temp = stack.pop();
                System.out.print(temp.data + " -> ");
                temp = temp.right;
            }

        }
    }

    public List<Integer> inOrderRecList(TreeNode root){
        List<Integer> list= new ArrayList<Integer>();
        if(root==null){
            return list;
        }
        return inorder(list,root);
    }
    public List inorder(List list, TreeNode root){
        if(root==null){
            return list;
        }
        inorder(list,root.left);
        list.add(root.data);
        inorder(list,root.right);
        return list;
    }

    //recursive in-order traversal
    public void inOrderRec(TreeNode root){
        if(root == null) {
            return;
        }
        inOrderRec(root.left);
        System.out.print(root.data +" -> ");
        inOrderRec(root.right);
    }

    public void createBinaryTree(){
        TreeNode first = new TreeNode(1);
        TreeNode second = new TreeNode(2);
        TreeNode third = new TreeNode(3);
//        TreeNode fourth = new TreeNode(4);
//        TreeNode fifth = new TreeNode(5);

        root = first;
//        first.left = second;
        first.right = second;
        second.left = third;
//        second.right = fifth;
    }


    class TreeNode{
        private TreeNode left;
        private TreeNode right;
        private int data;

        public TreeNode(int data){
            this.data = data;
        }
    }
}