package com.thread;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * wait 和notify 实现消费者
 * @Date 2020/4/20 18:16
 * @name Consumer
 */


public class Consumer implements Runnable{

    private Queue<Integer> queue;
    private int maxSize;

    public Consumer(Queue<Integer> queue, int maxSize) {
        this.queue = queue;
        this.maxSize = maxSize;
    }

    @Override
    public void run() {
        while(true){
            synchronized (queue){
                while(queue.isEmpty()){
                    try {
                        System.out.println("队列是空的 请等待!");
                        queue.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Integer remove = queue.remove();
                System.out.println("consumer num:"+remove);
                queue.notifyAll();
            }

        }
    }

    public static void main(String[] args) {
        Queue<Integer> queue=new LinkedList<>();
        Produer produer=new Produer(queue,5);
        Consumer consumer=new Consumer(queue,10);
        Thread p=new Thread(produer);
        Thread c=new Thread(consumer);
        p.start();
        c.start();
    }
}