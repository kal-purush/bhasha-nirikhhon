package com.issac.BinaryTree;

public class DoubleyLinkedList {
    public DoubleyNode head = new DoubleyNode(null);

    public void append(BinarySearchTree data){
        DoubleyNode currentNode = head;
        while(currentNode.next != null){
            currentNode = currentNode.next;
        }
        currentNode.next = new DoubleyNode(data);
        currentNode.next.prev = currentNode;
    }

    public void prepend(BinarySearchTree data){
        DoubleyNode tempNode = new DoubleyNode(data);
        tempNode.next = head.next;
        if(head.next == null){
            append(data);
        }
        head.next.prev = tempNode;
        head.next = tempNode;
    }

    public BinarySearchTree getValueAt(int index){
        DoubleyNode currentNode = head.next;
        for(int i=0;i<index;i++){
            currentNode = currentNode.next;
        }
        return currentNode.data;
    }

    public void deleteAt(int index){
        if(index == 0){
            head.next.prev = null;
            head.next = head.next.next;
            if(head.next != null) {
                head.next.prev = head;
            }
        } else {
            DoubleyNode currentNode = head.next;
            for (int i = 0; i < index-1; i++) {
                currentNode = currentNode.next;
            }
            currentNode.next = currentNode.next.next;
            if(currentNode.next != null) {
                currentNode.next.prev = currentNode;
            }
        }
    }

    public int getSize(){
        int size = 0;
        boolean loopStopper = true;
        DoubleyNode currentNode = head.next;
        if (head.next != null) {
            while (loopStopper) {
                if (currentNode.next == null) {
                    loopStopper = false;
                } else {
                    currentNode = currentNode.next;
                    size += 1;
                }
            }
            return size;
        }else{
            return size;
        }
    }

    @Override
    public String toString() {
        String output = "[";
        DoubleyNode currentNode = head.next;
        boolean loopStopper = true;
        while(loopStopper){
            output += currentNode.data + ", ";
            if(currentNode.next == null){
                loopStopper = false;
            }else{
                currentNode = currentNode.next;
            }
        }
        output = output.substring(0, output.length()-2) + "] [";
        while(currentNode != head && currentNode !=null){
            output += currentNode.data + ", ";
            currentNode = currentNode.prev;
        }
        output = output.substring(0, output.length()-2) + "]";
        return output;
    }
}