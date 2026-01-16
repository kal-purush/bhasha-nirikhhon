package hw.lesson4;

public interface LinkedList<E> {

    /**
     * O(1)
     * add element to the beginning of list
     * @param value
     */
    void insertFirst(E value);

    /**
     * O(1)
     * @return remove element from the beginning of list
     */
    E removeFirst();

    /**
     * O(n)
     * @param value
     * @return
     */
    boolean remove(E value);

    /**
     * O(n)
     * @param value
     * @return
     */
    boolean contains(E value);

    int size();

    boolean isEmpty();

    void display();

    E getFirst();


    class Node<E> {
        E item;
        Node<E> next;

        public Node(E item, Node<E> next) {
            this.item = item;
            this.next = next;
        }
    }
}