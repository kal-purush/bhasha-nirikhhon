package com.bridgelabz;

public class Queue {
    LinkedList<Integer> queue = new LinkedList<>();
    public void enqueue(Integer data) {
        queue.add(data);
    }
    public void dequeue() {
        while (queue.size() != 0) {
            queue.show();
            queue.popFirst();
        }
    }
    public void show() {
        queue.show();
    }
    public static void main(String[] args) {
        Queue queue1 = new Queue();
        queue1.enqueue(56);
        queue1.enqueue(30);
        queue1.enqueue(70);
        queue1.show();
        queue1.dequeue();
    }
}