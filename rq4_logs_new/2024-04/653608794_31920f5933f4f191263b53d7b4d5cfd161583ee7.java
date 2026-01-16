package com.xjdl.juc.cas;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yanglin
 * @create 2024-04-13 21:48 星期六
 * description:  自旋锁
 */
public class SpinLockDemo {
    
    AtomicReference<Thread> atomicReference = new AtomicReference<>();


    /**
     * 加锁
     */
    public void lock(){
        Thread thread = Thread.currentThread();
        //一直尝试抢锁
        while (!atomicReference.compareAndSet(null,thread)){
            
        }
        System.out.println(Thread.currentThread().getName() + "\t come in!");
    }

    /**
     * 解锁
     */
    public void unlock(){
        Thread thread = Thread.currentThread();
        //解锁
        atomicReference.compareAndSet(thread,null);
        System.out.println(Thread.currentThread().getName() + "\t task over!");
    }


    /**
     * 测试
     * @param args
     */
    public static void main(String[] args) {
        SpinLockDemo spinLockDemo = new SpinLockDemo();
        new Thread(() -> {
            //加锁
            spinLockDemo.lock();
            try {
                //A 线程业务
                System.out.println("Thread A 开始执行业务！");
                TimeUnit.SECONDS.sleep(5);
                System.out.println("Thread A 执行完成！");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            spinLockDemo.unlock();
        },"A线程").start();

        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        new Thread(() -> {
            spinLockDemo.lock();
            System.out.println("B 线程开始执行业务！");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("B 线程执行完成！");
            spinLockDemo.unlock();
        },"B线程").start();

    }
    
    
}