
import java.util.Iterator;

public class Deque<Item> implements Iterable<Item> {
    
    private Node first;
    
    private Node last;

    private int n;
    
    private class Node {
        private Item item;
        private Node next;
        private Node prev;
    }
    
    public Deque() {                          // construct an empty deque
        first = null;
        last = null;
        n = 0;
    }

    public boolean isEmpty() {                // is the deque empty?
        return n == 0;
    }

    public int size() {                      // return the number of items on the deque
        return n;
    }

    public void addFirst(Item item) {         // add the item to the front
        if (item == null) {
            throw new java.lang.NullPointerException();
        }
        Node oldfirst = first;
        first = new Node();
        first.item = item;
        first.next = oldfirst;
        first.prev = null;
        if (isEmpty()) { 
            last = first; 
        } 
        else { 
            oldfirst.prev = first; 
        }
        n++;
    }

    public void addLast(Item item) {          // add the item to the end
        if (item == null) {
            throw new java.lang.NullPointerException();
        }
        Node oldlast = last;
        last = new Node();
        last.item = item;
        last.next = null;
        last.prev = oldlast;
        if (isEmpty()) { 
            first = last; 
        } 
        else { 
            oldlast.next = last; 
        }
        n++;
    }

    public Item removeFirst() {               // remove and return the item from the front
        if (isEmpty()) {
            throw new java.util.NoSuchElementException();
        }
        Item item = first.item;
        first = first.next;
        n--;
        if (isEmpty()) {
            first = null;
            last = null;
        }
        else {
            first.prev = null;
        }
        return item;
    }

    public Item removeLast() {                 // remove and return the item from the end
        if (isEmpty()) {
            throw new java.util.NoSuchElementException();
        }
        Item item = last.item;
        last = last.prev;
        n--;
        if (isEmpty()) {
            first = null;
            last = null;
        } 
        else {
            last.next = null;
        }
        return item;
    }

    public Iterator<Item> iterator() {        // return an iterator over items in order from front to end
        return new DequeIterator();
    }

    private class DequeIterator implements Iterator<Item> {
        private Node current = first;

        public boolean hasNext() { return current != null; }

        public void remove() { throw new java.lang.UnsupportedOperationException(); }

        public Item next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            Item item = current.item;
            current = current.next;
            return item;
        }
    }

    public static void main(String[] args) {  // unit testing (optional)
        Deque<Integer> dq = new Deque<Integer>();
        System.out.printf("Is empty: %b \n", dq.isEmpty());
        System.out.printf("size: %d \n", dq.size());

        System.out.println("Adding...");
        dq.addFirst(2);
        dq.addLast(3);
        dq.addFirst(1);
        dq.addLast(4);
        for (int a: dq) {
            System.out.printf("%d ", a);
        }
        System.out.printf("Is empty: %b \n", dq.isEmpty());
        System.out.printf("size: %d \n", dq.size());
        System.out.println();
        
        System.out.println("Removing...");
        dq.removeFirst();
        dq.removeLast();
        for (int a: dq) {
            System.out.printf("%d ", a);
        }
        System.out.println();
    }
}