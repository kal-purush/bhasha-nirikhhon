package lockDemo;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @description:重用锁demo
 * @auth tongchenghao
 * @date 2020/2/4
 */
public class LockDome extends Thread {

    //重用读写锁
    static Lock lock = new ReentrantLock();//可以设置公平和非公平

    static int count = 0;

    @Override
    public void run() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        /**
         * synchronized (LockDome.class){
         *      count++;
         *  }
         *  与下面的代码功能一样
         *  区别：
         *  1.lock是类级别的实现，synchronized是jvm的关键字
         *  2.lock和unlock更加灵活，可以选择什么时候获得锁，什么时候释放锁；
         *      而 synchronized 的锁的获得是被动的，只有（1）当同步代码块执行完了才会释放锁；（2）当同步代码块出现异常时释放锁
         *  3.lock可以判断锁的状态，而 synchronized 作为关键字我们没有办法去作判断
         *  4.lock有公平锁和非公平锁；而 synchronized 是非公平锁
         */
        lock.lock();//获得锁
        count++;
        lock.unlock();//释放锁
    }

    public static void main(String[] args) {
        for (int i=0;i<1000;i++){
            new LockDome().start();
        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(count);
    }
}