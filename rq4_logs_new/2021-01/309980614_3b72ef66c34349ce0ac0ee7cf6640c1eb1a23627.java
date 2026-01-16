package com.java15.example.collections;

import java.util.Queue;

public class QueueAddDemo implements Runnable{
    final private Queue<? super String> queue;
    private  volatile int counter = 0;
    QueueAddDemo(Queue<? super String> queue){
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            queue.add("Hello" + (counter++));
        }
    }
}