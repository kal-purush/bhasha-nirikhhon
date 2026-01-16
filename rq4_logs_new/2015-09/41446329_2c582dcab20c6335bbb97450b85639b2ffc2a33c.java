/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TreeGeek;

import javax.print.DocFlavor;

/**
 *
 * @author manmindersinghsodhi
 */
public class CalculateSizeOfTree {
    
    private class Node
    {
        int data;
        Node left;
        Node right;
    }
    
    public Node newNode(int data)
    {
        Node pNode = new Node();
        pNode.data = data;
        pNode.left = null;
        pNode.right = null;
        return pNode;
    }
    
    
    //Recursive function
    public int size(Node pNode)
    {
        if(pNode==null)
        {
            return 0;
        }
        else
        {
            return (size(pNode.left) + 1 + size(pNode.right));
        }
    }
    
    
    public static void main (String args [])
    {
        CalculateSizeOfTree cs = new CalculateSizeOfTree();
        Node root = cs.newNode(1);
        root.left  = cs.newNode(2);
        root.right = cs.newNode(3);
        root.left.left = cs.newNode(4);
        root.left.right = cs.newNode(5);
        
        System.out.println("Size of the tree is " + cs.size(root));
        
    }
    
}