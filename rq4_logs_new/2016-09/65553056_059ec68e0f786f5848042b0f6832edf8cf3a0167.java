package default1;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Shiyi on 9/20/2016.
 */
public class LinkedListP<T> implements Iterable<T> {
    protected int pro = 0;//accessible by subclasses no matter where, and classes in the same package
    private int pri = 1;//accessible only in the class
    int pac = 2;//default access modifier: package private: accessible in same package
    protected static class Node<T> {
        T item;
        Node<T> next;
    }
    protected Node<T> head;
    public LinkedListP() {
        head = null;
    }
    public void addFirst(T item) {
        Node<T> newNode = new Node<>();
        newNode.item = item;
        newNode.next = head;
        head = newNode;
    }

    public void addLast(T item) {
        Node<T> currentNode = head;
        while(currentNode.next!=null) currentNode = currentNode.next;
        Node<T> tempNode = new Node<>();
        tempNode.item = item;
        tempNode.next = null;
        currentNode.next = tempNode;
    }

    public T pollFirst() {
        if (head==null) throw new NoSuchElementException("list is empty");
        T item = head.item;
        head = head.next;
        return item;
    }

    public T pollLast() {
        if (head==null) throw new NoSuchElementException("list is empty");
        T item;
        if (head.next==null) {
            item = head.item;
            head = null;
            return item;
        }
        if (head.next.next==null) {
            item = head.next.item;
            head.next = null;
            return item;
        }
        Node<T> currentNode = head;
        while(currentNode.next.next!=null) currentNode = currentNode.next;
        item = currentNode.next.item;
        currentNode.next = null;
        return item;
    }

    public void printList() {
        Node<T> currentNode = head;
        while(currentNode!=null) {
            System.out.println(currentNode.item);
            currentNode = currentNode.next;
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new LLIterator<>(head);
    }

    private class LLIterator<T> implements Iterator<T> {
        //private Node<T> current;
        private Node<T> current;
        public LLIterator(Node<T> head) {
            current = head;
        }
        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public T next() {
            if (!this.hasNext())
                throw new NoSuchElementException("by LinkedListP");
            T tempItem = current.item;
            current = current.next;
            return tempItem;
        }
    }

    public static void main(String[] args) {
        LinkedListP<Integer> list = new LinkedListP<>();
        list.addFirst(1);
        list.addFirst(2);
        list.addFirst(3);
        list.addLast(0);
        System.out.println(list.pollFirst());
        System.out.println(list.pollLast());
        list.printList();
        for(Integer i:list) System.out.println(i);
    }
}