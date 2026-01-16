package com.linked_list;

import java.util.NoSuchElementException;

public class CircularlyLinkedList<E> {
    private class Node<E> {
        private E value;
        private Node next;

        public Node(E value) {
            this.value = value;
        }
    }

    private Node<E> last;
    private int size = 0;

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public E first() {
        if (isEmpty())
            throw new NoSuchElementException();
        return (E) last.next.value;
    }

    public E last() {
        if (isEmpty())
            throw new NoSuchElementException();
        return last.value;
    }

    public void rotate() {
        if (last != null)
            last = last.next;
    }

    public void addFirst(E item) {
        var node = new Node<>(item);
        if (isEmpty()) {
            last = node;
            last.next = last;
            size++;
        } else {
            node.next = last.next;
            last.next = node;
            size++;
        }
    }

    public void addLast(E item) {
        addFirst(item);
        last = last.next;
    }

    public void removeFirst() {
        if (isEmpty())
            throw new NoSuchElementException();
        if (size == 1) {
            last = null;
            size--;
            return;
        }
        var second = last.next;
        last.next = second.next;
        size--;
    }
}