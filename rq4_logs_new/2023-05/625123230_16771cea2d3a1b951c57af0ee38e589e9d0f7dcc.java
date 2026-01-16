package com.example._2023practice.common.collection;

public class LinkedListV {
public Node head;

    @Override
    public String toString() {
        return "LinkedListV{" +
                "head=" + head +
                '}';
    }

    public void addToHead(String data){

    Node n=new Node(data);
    n.nextnode=head;
    head=n;
}
public void add(String data){
    Node  n=new Node(data);
    Node currentNode = head;
    Node y =null;
    if(head==null){
        head=n;
    }else{
        while(currentNode.nextnode!=null){
           currentNode=currentNode.nextnode;
           }
currentNode.nextnode=n;

    }
}
public boolean iscyle(){
        Node slowRef=head;
        Node fastRef=head;

        while((slowRef!=null && fastRef!=null && fastRef.nextnode!=null)){
            fastRef.nextnode=fastRef.nextnode.nextnode;
            slowRef.nextnode=slowRef.nextnode;

            if(fastRef==slowRef)return true;
        }
        return false;
}
    public void add(Node data){
        Node  n=data;
        Node currentNode = head;
        Node y =null;
        if(head==null){
            head=n;
        }else{
            while(currentNode.nextnode!=null){
                currentNode=currentNode.nextnode;
            }
            currentNode.nextnode=n;

        }
    }


    public static class Node{
        public String data;
        public Node nextnode;

        @Override
        public String toString() {
            return "Node{" +
                    "data='" + data + '\'' +
                    ", nextnode=" + nextnode +
                    '}';
        }

        public Node(String data) {
            this.data = data;
            this.nextnode=null;
        }
    }
}