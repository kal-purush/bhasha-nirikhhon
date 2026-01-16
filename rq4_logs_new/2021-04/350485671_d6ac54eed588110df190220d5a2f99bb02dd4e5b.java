package challenges.stacksQueues;

import java.util.NoSuchElementException;

public class CircularBuffer<T> implements BoundedQueue<T> {
    int capacity;
    int front = 0;
    int back = 0;
    boolean full = false;
    private final Object[] items;

    public CircularBuffer(int capacity) {
        this.capacity = capacity;
        if (capacity < 1) throw new IllegalArgumentException("Circular buffer must have positive capacity");
        items = new Object[capacity];
    }

    @Override
    public boolean isEmpty() {
        return front == back;
    }

    @Override
    public boolean isFull() {
        return full;
    }

    @Override
    public boolean offer(T item) {
        if (isFull()) return false;
        items[back] = item;
        back = back == capacity - 1 ? 0 : back + 1;
        if (front == back) full = true;
        return true;
    }

    @Override
    public void enqueue(T item) throws CollectionIsFullException {
        if (isFull()) throw new CollectionIsFullException("Circular Buffer is full.");
        items[back] = item;
        back = back == capacity - 1 ? 0 : back + 1;
        if (front == back) full = true;
    }

    @Override
    public T dequeue() throws NoSuchElementException {
        if (isEmpty()) throw new NoSuchElementException("Circular Buffer is empty.");
        full = false;
        T item = (T) items[front];
        front = front == capacity - 1 ? 0 : front + 1;
        return item;
    }
}