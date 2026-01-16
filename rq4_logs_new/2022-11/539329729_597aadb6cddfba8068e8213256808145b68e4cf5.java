package ru.nsu.kgurin;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * BFS algorithm for traversing a tree in depth.
 */
public class BreadthFirstSearchIterator<T> implements Iterator<Node<T>> {
    Queue<Node<T>> queue = new LinkedList<>();
    private int modCount;

    /**
     * Initialization of BFS Iterator.
     *
     * @param vertex vertex from DFS is executed
     */
    public BreadthFirstSearchIterator(Node<T> vertex) {
        queue.add(vertex);
        modCount = vertex.getModCount();
    }

    /**
     * Check if the next node exists.
     *
     * @return true if there is next vertex, false otherwise
     */
    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * Switch to the next node of the tree.
     *
     * @return the current node
     */
    @Override
    public Node<T> next() {
        Node<T> next = queue.poll();
        if (this.modCount != next.getModCount()) {
            throw new ConcurrentModificationException("Changing structure while iterating");
        }
        queue.addAll(next.getChildren());
        return next;
    }
}