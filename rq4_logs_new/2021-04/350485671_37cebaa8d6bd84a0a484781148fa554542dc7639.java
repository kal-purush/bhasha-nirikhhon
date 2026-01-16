package challenges.stacksQueues;

import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class TwoStacksQueue<T, S extends Stack<T>> implements Queue<T> {
    S front;
    S back;

    public TwoStacksQueue (Supplier<S> cons) {
        front = cons.get();
        back = cons.get();
    }

    public boolean isEmpty() {
        return front.isEmpty() && back.isEmpty();
    }

    @Nullable
    public T peek() {
        if (isEmpty()) return null;
        if (front.isEmpty()) overturn();
        return front.peek();
    }

    @Override
    public void enqueue(T item) {
        back.push(item);
    }

    private void overturn() {
        while (!back.isEmpty()) front.push(back.pop());
    }

    @Override
    public T dequeue() throws NoSuchElementException {
        if (isEmpty()) throw new NoSuchElementException("Queue has no elements");
        if (front.isEmpty()) overturn();
        return front.pop();
    }
}