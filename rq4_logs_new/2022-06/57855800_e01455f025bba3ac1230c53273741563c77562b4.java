package com.github.lucaseo90.ds.day09.stackNqueue;

import java.util.Stack;

public class No0232 {

    private Stack<Integer> stackA = new Stack<>();
    private Stack<Integer> stackB = new Stack<>();

    public No0232() {

    }

    public void push(int x) {
        while (!stackB.isEmpty()) {
            stackA.push(stackB.pop());
        }
        stackA.push(x);
    }

    public int pop() {
        while (!stackA.isEmpty()) {
            stackB.add(stackA.pop());
        }
        return stackB.pop();
    }

    public int peek() {
        while (!stackA.isEmpty()) {
            stackB.add(stackA.pop());
        }
        return stackB.peek();
    }

    public boolean empty() {
        return (stackA.isEmpty() && stackB.isEmpty());
    }

}