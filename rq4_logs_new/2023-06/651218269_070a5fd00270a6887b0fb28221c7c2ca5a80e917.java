package datastructures.stacks;

import java.util.ArrayList;

/**
 * Stack through array based implementation.
 * @param <T> - data type which will be stored in the stack.
 */
public class StackArray<T> implements StackADT<T> {
    private int size;
    private final ArrayList<T> stack;

    public StackArray() {
        size = 0;
        stack = new ArrayList<>();
    }

    /**
     * Get the size of the stack.
     * Time complexity: O(1).
     * @return int size - current depth of the stack.
     */
    public int size() {
        return size;
    }

    /**
     * Check if the stack is empty.
     * Time complexity: O(1).
     * @return boolean - true if stack is empty; Otherwise, false.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Push object to the top of the stack.
     * Time complexity: O(1).
     * @param obj - received object. Will be the head of the stack.
     */
    @Override
    public void push(T obj) {
        stack.add(obj);
        size++;
    }

    /**
     * Delete the object from the top of the stack.
     * Current stackHead will point to the predecessor (if exist).
     * Time complexity: O(1).
     */
    @Override
    public void pop() {
        if (isEmpty()) {
            return;
        }

        stack.remove(size - 1);
        size--;
    }

    /**
     * Gain the object from the top of the stack.
     * Time complexity: O(1).
     * @return T obj - object from the top of the stack without removing.
     */
    @Override
    public T peek() {
        if (isEmpty()) {
            return null;
        }

        return stack.get(size - 1);
    }
}