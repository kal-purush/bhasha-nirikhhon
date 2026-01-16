import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author rkaul7, mclayton34
 * @version 0.1
 * @param <E> the type of object contained in this list
 */
public class MyLinkedList<E> implements SimpleList<E>, Iterable<E> {
    private Node<E> head;
    private int size;

    /**
     * @return number of elements in this list
     */

    @Override
    public int size() {
        return size;
    }

    /**
     * @return true if the list contains no elements
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Appends the specified element at the end of the list.
     *
     * @param e element to be inserted
     * @return true if the list has been modified, false otherwise
     */

    @Override
    public boolean add(E e) {
        Node<E> newNode = new Node<E>(e);
        if (head == null) {
            head = newNode;
        } else {
            Node<E> probe = head;
            while (probe.getNext() != null) {
                probe = probe.getNext();
            }
            probe.setNext(newNode);
        }
        size++;
        return true;
    }

    /**
     * Removes all of the elements from this list
     * The list will be empty after this call returns.
     */

    @Override
    public void clear() {
        head = null;
        size = 0;
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index of the element to return
     * @return element at the specified position in the list
     */
    @Override
    public E get(int index) {
        if (index >= size) {
            throw new NoSuchElementException();
        }
        Node<E> probe = head;
        for (int i = 0; i < index; i++) {
            probe = probe.getNext();
        }
        return probe.getData();
    }

    @Override
    public Iterator<E> iterator() {
        INeedPoints<E> points = new INeedPoints<E>();
        return points;
    }

    private class INeedPoints<E> implements Iterator<E> {
        private int cursor = 0;
        private boolean check = true;

        @Override
        public boolean hasNext() {
            return this.cursor < size();
        }

        @Override
        public E next() {
            check = false;
            if (!(hasNext())) {
                throw new NoSuchElementException();
            }
            return (E) get(cursor++);
        }
        @Override
        public void remove() {
            if (check) {
                throw new IllegalStateException();
            }
            Node<E> toRemove = (Node<E>) head;
            for (int i = 1; i < cursor; i++) {
                toRemove = toRemove.getNext();
            }
            Node<E> replace = (Node<E>) head;
            for (int i = 1; i < cursor; i++) {
                replace = replace.getNext();
            }
            for (int j = cursor; j < size(); j++) {
                replace = replace.getNext();
                toRemove.setNext(replace);
            }
            size--;
            check = true;
        }
    }


    //****** Do not modify anything below this line! ********

    /**
     * For grading, returns the head
     *
     * @return the head of the list
     */
    public Node<E> getHead() {
        return head;
    }

    /**
     * @author mclayton34
     * @version 0.1
     * @param <E> the type of object contained in this node
     */
    public class Node<T> {
        private T data;
        private Node<T> next;

        /**
         * Constructor for node, creates a node with data
         * equal to parameter data
         *
         * @param data the data to exist in the node
         */
        public Node(T data) {
            this.data = data;
        }

        /**
         * @param data constructs a node with the reference
         *             to the next node already included
         * @param next the next node
         */
        public Node(T data, Node<T> next) {
            this(data);
            this.next = next;
        }

        /**
         * @return the next node from this one
         */
        public Node<T> getNext() {
            return next;
        }

        /**
         * sets the next node for this node
         *
         * @param next the node to set as next
         */
        public void setNext(Node<T> next) {
            this.next = next;
        }

        /**
         * Gets the data for this node
         *
         * @return the data for this node
         */
        public T getData() {
            return data;
        }
    }
}