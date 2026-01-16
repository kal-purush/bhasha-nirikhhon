
package com.DLL.ds.linkedlist;

public class DoublyLinkedListImpl<E> {
    private Node head;
    private Node tail;

    private class Node {
        E element;
        Node next;
        Node prev;

        public Node(E element, Node next, Node prev) {
            this.element = element;
            this.next = next;
            this.prev = prev;
        }
    }

    public void addFirst(E element) {
        Node tmp = new Node(element, head, null);
        if (head != null) {
            head.prev = tmp;
        }
        head = tmp;
        if (tail == null) {
            tail = tmp;
        }
        System.out.println("adding: " + element);
    }

    public void addLast(E element) {
        Node tmp = new Node(element, null, tail);
        if (tail != null) {
            tail.next = tmp;
        }
        tail = tmp;
        if (head == null) {
            head = tmp;
        }
        System.out.println("adding: " + element);
    }

    public E removeFirst() {
        Node tmp = head;
        head = head.next;
        head.prev = null;
        System.out.println("deleted: " + tmp.element);
        return tmp.element;
    }


    public E removeLast() {
        Node tmp = tail;
        tail = tail.prev;
        tail.next = null;
        System.out.println("deleted: " + tmp.element);
        return tmp.element;
    }

    public Node search(E data) {
        Node temp = head;
        if (head == null) System.out.println("Empty list.");
        else {
            while (temp != null && temp.element != data) {
                temp = temp.next;
            }
            if (temp.element != data) System.out.println("not in the list.");
            else return temp;
        }

        return temp;
    }

    public static void main(String[] args) {
        DoublyLinkedListImpl<Integer> dll = new DoublyLinkedListImpl<Integer>();
        dll.addFirst(2);
        dll.addFirst(3);
        dll.addLast(5);
        dll.addLast(7);
        dll.removeFirst();
        dll.removeLast();
        try {
            System.out.println(dll.search(5).element);
        } catch (NullPointerException e){

        }
    }
}

