/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package BinarySearchTree;

import java.util.Scanner;

/**
 *
 * @author manmindersinghsodhi
 */
public class BinarySearchTree {
    
    int nCount, lCount,level;
    Node root = null;
    private class Node
    {
        Node left, right;
        int data;
    }
    
    public void insert (int x)
    {
        Node pNode =new Node();
        Node p = null;
        Node follow = null;
        if(pNode==null)
        {
            System.out.println("Cannot insert, Memory Overflow");
        }
        
        else
        {
            pNode.data = x;
            pNode.left = p.right = null;
           if(root==null)
           {
               root= pNode;
           }
           else
           {
               p = root;
               while(p!=null)
               {
                   follow = p;
                   if(pNode.data<p.data)
                       p = p.left;
                   else
                       p = p.right;
               }
               
               if(pNode.data<follow.data)
                   follow.left = pNode;
               else
                   follow.right = pNode;
           
            }
        
        }
    }

    
    public void create ()
    {
        int i, n , x;
        
        System.out.println("Please enter number of elements");
        Scanner read = new Scanner(System.in);
        
        n = read.nextInt();
        
        System.out.println("Please enter the elements");
        for(i = 0;i<n; i++)
        {
            x  = read.nextInt();
            insert(x);
        }
    }
    
    public void preOrder(Node root)
    {
        if(root!=null)
        {
            System.out.println(root.data);
            preOrder(root.left);
            preOrder(root.right);
        }
    }
    
    public void inOrder(Node root)
    {
        if(root!=null)
        {
            inOrder(root.left);
            System.out.println(root.data);
            inOrder(root.right);
        }
    }
    
    public void postOrder (Node root)
    {
        if(root!=null)
        {
            postOrder(root.left);
            postOrder(root.right);
            System.out.println(root.data);
        }
    }
    
    public void search (int x)
    {
        Node p;
         p =root;
         while(p!=null)
         {
             if(p.data==x)
             {
                 break;
             }
             if(p.data<x)
                 p = p.left;
             else
                 p = p.right;
         }
         
         if(p==null)
         {
             System.out.println("Required node not found");
             
         }
         
         else
         {
             System.out.println("Required node found " + p);
         }
    }
    
    
    public void levelNode (int k)
    {
        Node p = root;
        
        while(p!=null)
        {
            if(p.data==k)
                break;
            level++;
            if(k<p.data)
                p = p.left;
            else
                p = p.right;
                
        }
        
        if(p==null)
        {
             System.out.println("Required node not found");
             
         }
         
         else
         {
             System.out.println("Required node found " + p);
         }
    }
    
    
    
    
    public static void main (String [] args)
    {
        
    }
    
}