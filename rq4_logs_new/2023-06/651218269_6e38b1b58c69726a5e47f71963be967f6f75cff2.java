package datastructures.stacks;

import datastructures.stacks.Stack;

/**
 * Stack through linked list implementation.
 * @param <T> - data type which will be stored in the stack.
 */
public class StackLL<T> implements Stack<T> {
    private int size;
    private ObjectRefPair<T, ObjectRefPair> stackHead;

    StackLL() {
        size = 0;
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
     * Time conplexity: O(1).
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
        if (isEmpty()) {
            stackHead = new ObjectRefPair<>(obj, null);
            size++;
            return;
        }

        stackHead = new ObjectRefPair<>(obj, stackHead);
        size++;
    }

    /**
     * Delete the object from the top of the stack. Current stackHead will point to the predecessor (if exist).
     * Time complexity: O(1).
     */
    @Override
    public void pop() {
        stackHead = stackHead.getRef();
        size--;
    }

    /**
     * Gain the object from the top of the stack.
     * Time complexity: O(1).
     * @return T obj - object from the top of the stack without removing.
     */
    @Override
    public T peek() {
        return stackHead.getObj();
    }
}

/**
 * Class, which implement the pair of object and reference to another object.
 * @param <O> - object.
 * @param <R> - reference to another object.
 */
class ObjectRefPair<O, R> {
    private final O obj;
    private final R ref;

    ObjectRefPair(O objReceived, R refReceived) {
        this.obj = objReceived;
        this.ref = refReceived;
    }

    /**
     * Get O object from the pair.
     * @return O obj - object from the pair.
     */
    public O getObj() {
        return obj;
    }

    /**
     * Get R reference from the pair.
     * @return R ref - reference to another object from the pair.
     */
    public R getRef() {
        return ref;
    }
}