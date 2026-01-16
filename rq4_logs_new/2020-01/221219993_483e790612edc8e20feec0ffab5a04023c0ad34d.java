package edu.kis.vh.nursery.ListIntegers;

public class IntArrayStack implements ListInterface {

    public int MAX_AMOUNT_OF_ELEMENTS = 12;
    final public int[] numbers = new int[MAX_AMOUNT_OF_ELEMENTS];

    private int EMPTY = -1;
    private int VALUE_IF_LIST_EMPTY = 0;


    @Override
    public void push(int in) {
        if (!isFull())
            numbers[++EMPTY] = in;
    }


    @Override
    public boolean isEmpty() {
        return EMPTY == -1;
    }


    public boolean isFull() {
        return EMPTY == MAX_AMOUNT_OF_ELEMENTS - 1;
    }


    @Override
    public int top() {
        if (isEmpty())
            return VALUE_IF_LIST_EMPTY;
        return numbers[EMPTY];
    }


    @Override
    public int pop() {
        if (isEmpty())
            return VALUE_IF_LIST_EMPTY;
        return numbers[EMPTY--];
    }


    public int getTotal() {
        return EMPTY;
    }

}