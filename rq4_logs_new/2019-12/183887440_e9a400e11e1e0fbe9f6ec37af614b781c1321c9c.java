package com.yw.juc;

import java.util.concurrent.locks.LockSupport;

/**
 * Created by 18765 on 2019/12/8 10:31
 *
 *  LockSupport 可响应中断，可以终止任意线程，可以唤醒指定线程。
 *
 */
public class LockSupportDemo {
    static Object o = new Object();
    static MyselfThread t1 = new MyselfThread("t1");
    static MyselfThread t2 = new MyselfThread("t2");
    static class MyselfThread extends Thread{

        public MyselfThread(String name) {
            super(name);
        }
        @Override
        public void run() {
            synchronized (o){
                System.out.println("in :"+this.getName());
                LockSupport.park();
                if(Thread.interrupted()){
                    System.out.println(this.getName()+"被中断了");
                }
            }
            System.out.println("执行结束："+this.getName());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        t1.start();
        Thread.sleep(100);
        t2.start();
        t1.interrupt();
        LockSupport.unpark(t2);
    }
}