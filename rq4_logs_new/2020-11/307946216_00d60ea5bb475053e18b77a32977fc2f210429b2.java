package hw.lesson3;

import hw.lesson3.dequeue.Dequeue;
import hw.lesson3.dequeue.DequeueImpl;
import hw.lesson3.queue.PriorityQueue;
import hw.lesson3.queue.Queue;
import hw.lesson3.queue.QueueImpl;
import hw.lesson3.stack.Stack;
import hw.lesson3.stack.StackImpl;

public class TestClass {

    public static void main(String[] args) {
//        testStack();
//       testQueue();
        testDequeue();
    }

    private static void testStack() {
        Stack<Integer> stack = new StackImpl<>(5);

        System.out.println("Add value 1: " + addToStack(stack, 1));
        System.out.println("Add value 2: " + addToStack(stack, 2));
        System.out.println("Add value 3: " + addToStack(stack, 3));
        System.out.println("Add value 4: " + addToStack(stack, 4));
        System.out.println("Add value 5: " + addToStack(stack, 5));
        System.out.println("Add value 6: " + addToStack(stack, 6));

        System.out.println("Stack size: " + stack.size());
        System.out.println("Stack top: " + stack.peek());

        while (!stack.isEmpty()) {
            System.out.println(stack.pop());
        }
    }

    private static boolean addToStack(Stack<Integer> stack, int value) {
        if (!stack.isFull()) {
            stack.push(value);
            return true;
        }
        return false;
    }

    private static void testQueue() {
        Queue<Integer> queue = new QueueImpl<>(5);
//        Queue<Integer> queue = new PriorityQueue<>(5);
        System.out.println(queue.insert(3));
        System.out.println(queue.insert(5));
        System.out.println(queue.insert(1));
        System.out.println(queue.insert(2));
        System.out.println(queue.insert(6));
        System.out.println(queue.insert(4));

        System.out.println("Queue peek: " + queue.peekHead());
        System.out.println("Queue size: " + queue.size());
        System.out.println("Queue is full: " + queue.isFull());

        while (!queue.isEmpty()) {
            System.out.println(queue.remove());
        }
    }

    private static void testDequeue(){

        Dequeue<Integer> dequeue = new DequeueImpl<>(9);

        for (int i = 0; i < 5; i++) {
            System.out.println(dequeue.insertFront(i));
        }
        for (int i = 0; i < 5; i++) {
            System.out.println(dequeue.insertBack(i));
        }
        System.out.println(dequeue.removeBack());
        System.out.println(dequeue.removeFront());

    }
}