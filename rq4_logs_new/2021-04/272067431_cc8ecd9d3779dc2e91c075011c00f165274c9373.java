/*
"Palindrome: Implement a function to check if a linked list is a palindrome."
github.com/J-Nying
*/

import java.io.*;
import java.util.*;
import javax.swing.*;
import java.util.Arrays;


public class palindromeLinkedList{

   public static void main(String[] args)
   {
   
      Scanner input = new Scanner(System.in);
      System.out.println("Enter the size of the Linked List:");
      int size = input.nextInt();
      LinkedList list = new LinkedList();
      int count = 0;
      
      while(count<size){
      
         System.out.println("Enter value number " + (count+1) + ":");
         int newNode = input.nextInt();
         list = appendNode(list, newNode);
         count++;

      }
      
      System.out.println("Here is the Linked List:");
      printLinkedList(list);

      Node [] array = convertLinkedListToArray(list, size);
      if(oh(array)==1 || size==1){
        System.out.println("\nIt is a palindrome.");
      }
      else{
        System.out.println("\nIt is not a palindrome.");

      }
      
   }



   public static Node[] convertLinkedListToArray(LinkedList l, int s){
        
    Node n=l.head;
    Node[] nodeArray=new Node[s];


    for(int i=0; i<s; i++){
        nodeArray[i]=n;
        n=n.next;
    }

    return nodeArray;
    
}

public static int oh(Node[] n){
    int bool=1;

         for(int i=0; i<=(n.length)/2; i++){
                if(n[i]!=n[n.length-1-i]){
                    bool=0;
                    break;
                }
          }
         return bool;

}

    


   public static LinkedList appendNode(LinkedList l, int d){
      Node newNode=new Node(d);
      
      if(l.head==null)
      {
         l.head=newNode;            
      }      
      else{
         Node currentNode=l.head;
         while(currentNode.next!=null)
         {
            currentNode=currentNode.next;
         }
         
         if(currentNode.next==null)
         {
            currentNode.next=newNode;
         }
      }
      
      return l;
   }
      
      
      
      
   public static void printLinkedList(LinkedList l){
      Node currentNode=l.head;
      
      while(currentNode.next!=null){
         System.out.print(currentNode.data + " -> ");
         currentNode=currentNode.next;
      }
      
      System.out.print(currentNode.data + "");                       
   }


}


class LinkedList{
   Node head;
}

class Node{
   int data;
   Node next;
   
   Node(int d){
      data = d;
   }

}