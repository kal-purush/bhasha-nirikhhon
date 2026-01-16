/*
 * Singleton pattern. 
 * To make sure there is only one instance of class
 * 
 */

package com.lsj.pattern;


class Wife {
    
    private volatile static Wife wife;
    private static int instanceCount = 0;
    
    private Wife() {
        System.out.println("Unique wife got");
        instanceCount++;
    }
    
    public static Wife getWife() {
        if (wife == null) {
            synchronized (Wife.class) {
                if (wife == null) {
                    wife = new Wife();
                }
            }
        }
        return wife;
    }

    public static int getInstanceCount() {
        return instanceCount;
    }

}

public class SingletonDemo {

    public static void main(String[] args) {
        Runnable runnable = new Runnable() {
            
            @Override
            public void run() {
                Wife.getWife();
                System.out.println("Thread: " + Thread.currentThread().getId() + ", wife count: " + Wife.getInstanceCount());
            }
        };
        for (int i = 0; i < 1000; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
        
    }

}