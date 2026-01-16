package com.singly_link_list;

public class LinkList<T extends Comparable> {
    static Node head;
    private int size = 0;
    private int index = 0;

    public boolean isEmpty() {
        return (this.head == null);
    }

    public int size() {
        return size;
    }

    public void append(T item) {
        if (head == null) {
            this.head = head;
            size++;
        } else {
            Node findLast = head;
            while (findLast.next != null) {
                findLast = findLast.next;
            }
            findLast.next = new Node(item);
            size++;
        }
    }

    public void insert(int pos, T item) {
        Node node = head;
        Node prev = null;
        while (node.next != null) {
            index++;
            prev = node;
            node = node.next;
        }
        Node newNode = new Node();
        newNode.data = item;

        newNode.next = node;
        if(index == 0){
            head = newNode;
        } else {
            prev.next = newNode;
        }
    }

    public static class Node<T extends Comparable> {
        T data;
        Node next;

        Node(T d) {
            this.data = d;
            this.next = null;
        }

        public Node() {

        }
    }


    public void add(T item) {
        Node<T> node = new Node(item);
        if (head == null) {
            this.head = node;
            size++;
        } else {
            Node findLast = head;
            while (findLast.next != null) {
                findLast = findLast.next;
            }
            findLast.next = new Node(item);
            size++;
        }

    }

    public Boolean search(T item) {
        try {
            if (head == null) {
                this.head = head;
                index++;
            }
            Node findElement = head;
            while (findElement.next != null) {
                index++;
                if (findElement.data == item) {
                    return true;
                }
                findElement = findElement.next;
            }
            return  false;
        } catch (NullPointerException e) {
            return true;
        }

    }
    public int index(T item) {
        search(item);
        return index;
    }



    public void pop() {

        if (head == null) {
            System.out.println("Empty list.");
        } else {
            Node secondLast = head;
            while (secondLast.next.next != null) {
                secondLast = secondLast.next;
            }
            secondLast.next = null;
        }
    }

    @Override
    public String toString() {
        StringBuilder element = new StringBuilder();
        Node<T> findLast = head;
        while (findLast.next != null) {
            element.append(" ").append(findLast.data).append(",");
            findLast = findLast.next;
        }
        element.append(" ").append(findLast.data).append(" ");
        System.out.println("["+element+"]");
        return "["+element+"]";
    }


}