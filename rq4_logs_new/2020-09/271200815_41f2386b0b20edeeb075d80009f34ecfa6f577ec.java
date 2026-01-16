package com.practice.hackerRank.queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ImplementQueue {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int test = Integer.parseInt(br.readLine());
        Queue queue = new Queue();
        while (test > 0){
            String s = br.readLine();
            implementQueue(s, queue);
            test--;
        }
    }

    private static void implementQueue(String s, Queue queue) {
        String[] arr = s.split(" ");
        if (arr[0].equals("Enqueue")){
            int n = Integer.parseInt(arr[1]);
            queue.enqueue(n);
        }
        else
            System.out.println(queue.dequeue());
    }
}

class Queue{
    int[] queue;
    int front;
    int rear;
    Queue(){
        queue = new int[10000];
        front = 0;
        rear = -1;
    }
    void enqueue(int n){
        rear++;
        queue[rear] = n;
    }
    String  dequeue(){
        if (front > rear)
            return "Empty";
        int n = queue[front];
        front++;
        return Integer.toString(n);
    }
}