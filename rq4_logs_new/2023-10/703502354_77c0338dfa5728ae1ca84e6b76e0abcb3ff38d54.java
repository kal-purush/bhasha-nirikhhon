
import java.util.Arrays;

public class Queue {

    private int maxSize;
    private String[] queueArray;
    private int front;
    private int rear;
    private int currentSize;

    public Queue(int size) {
        maxSize = size;
        queueArray = new String[maxSize];
        front = 0;
        rear = -1;
        currentSize = 0;
    }

    public boolean isEmpty() {
        return currentSize == 0;
    }

    public boolean isFull() {
        return currentSize == maxSize;
    }

    public int size() {
        return currentSize;
    }

    public void enqueue(String item) {
        if (!isFull()) {
            rear = (rear + 1) % maxSize;
            queueArray[rear] = item;
            currentSize++;
            System.out.println("Enqueued: " + item);
        } else {
            System.out.println("Queue is full. Cannot enqueue " + item);
        }
    }

    public String dequeue() {
        if (!isEmpty()) {
            String removedItem = queueArray[front];
            queueArray[front] = null;
            front = (front + 1) % maxSize;
            currentSize--;
            System.out.println("Dequeued: " + removedItem);
            return removedItem;
        } else {
            System.out.println("Queue is empty. Cannot dequeue.");
            return null;
        }
    }

    public String peek() {
        if (!isEmpty()) {
            return queueArray[front];
        } else {
            System.out.println("Queue is empty. Cannot peek.");
            return null;
        }
    }

    public String rear() {
        if (!isEmpty()) {
            return queueArray[rear];
        } else {
            System.out.println("Queue is empty.");
            return null;
        }
    }

    public void printArray() {

        System.out.print("List: ");
        System.out.print("[");
        int count = 0;
        for (int i = 0; count < currentSize; i++) {
            if (queueArray[i] != null) {
                System.out.print(queueArray[i]);
                count++;
                if (count < currentSize) {
                    System.out.print(", ");
                }
            }
        }
        System.out.println("]");
    }

}