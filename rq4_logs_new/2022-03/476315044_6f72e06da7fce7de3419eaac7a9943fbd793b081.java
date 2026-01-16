package com.bridgelabz;

public class LinkedList {
    Node head;
    Node tail;

    public void add(int data){
        Node newNode = new Node(data);
        if (head == null)
            head = newNode;
        else
            tail.next = newNode;
        tail = newNode;
    }


    public void display(){
        Node temp = head;
        while (temp != null){
            System.out.print( temp.key +"->" );
            temp = temp.next;
        }
    }
}