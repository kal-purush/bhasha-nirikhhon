package dataStructures.queue;

import dataStructures.sharedNode.Node;

import java.util.EmptyStackException;

public class Queue {
    private Node front;
    private Node back;

    public Queue() {
        front = null;
        back = null;
    }

    public static void main(String[] args) {
        Queue queue = new Queue();
        queue.enqueue(3);
        queue.enqueue(5);
        queue.enqueue(8);
        queue.enqueue(4);
        System.out.println(queue.print());

    }

    public boolean isEmpty() {
        return (front == null && back == null);
    }

    public void enqueue(int value) {
        Node node = new Node(value);
        if (isEmpty()) {
            front = node;
            back = node;
        }
        back.setNext(node);
        back = back.getNext();
    }

    public void dequeue() {
        if (isEmpty()) {
            throw new EmptyStackException();
        }
        front = front.getNext();
    }

    public int peek() {
        if (isEmpty()) {
            throw new RuntimeException("The Queue is Empty");
        }
        return front.getData();
    }

    public String print() {
        StringBuilder values = new StringBuilder();

        if (!isEmpty()) {
            Node curr = front;
            while (curr != back.getNext()) {
                if (curr == back) {
                    values.append(curr.getData());
                    return values.toString();
                }
                values.append(curr.getData()).append(" , ");
                curr = curr.getNext();
            }
        }
        return values.toString();
    }
}