package com.company;

public class Main {

    public static void main(String[] args) throws InterruptedException {
//        MyThread myThread = new MyThread("myThread");
//        myThread.start();

//        Thread myThread = new Thread(new MyRunnable(), "myRunnable");
//        myThread.start();
//        System.out.println("Running the thread");
//        myThread.join();
//        System.out.println("Thread is done....");

        Thread[] threads = new Thread[5];
        for(int i = 0; i < threads.length;i++){
            threads[i] = new Thread(new JoinExample(),"JoinedThreads"+i);
            threads[i].start();
        }
        for(int i = 0; i < threads.length;i++){
            threads[i].join();
        }

        System.out.println("["+Thread.currentThread().getName()+
                "] All threads have finished.");
    }
}