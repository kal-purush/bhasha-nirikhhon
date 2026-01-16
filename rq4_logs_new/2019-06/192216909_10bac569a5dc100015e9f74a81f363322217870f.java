package com.juc.demo.semaphore;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 信号量，控制窗口排队办理业务
 * @Auther: wangjian
 */
public class SemaphoreDemo {

    public static void main(String[] args)  {
        Semaphore semaphore = new Semaphore(2, true); // 2个窗口，默认为非公平锁，即不按线程排队顺序执行
        System.out.println("可办理业务窗口总数:" + semaphore.availablePermits());
        for (int i = 0; i < 10; i++) {
            new Thread(()-> {
                try {
                    boolean bFlag = true;
                    System.out.println();
//                    semaphore.acquire(); // 获取资源请求,查看要否有空余窗口,愿意等到天荒地老的忠实客户
                    bFlag = semaphore.tryAcquire(3, TimeUnit.SECONDS);// 就等 3 秒钟的傲娇客户
                    if (bFlag) {
                        System.out.println("当前等待办理业务人数: " + semaphore.getQueueLength());
                        System.out.println(Thread.currentThread().getName() + "正在办理业务，剩余窗口" + semaphore.availablePermits());
                        TimeUnit.SECONDS.sleep(2);
                        semaphore.release(); // 释放资源占用，业务办理结束离开办理窗口
                        System.out.println(Thread.currentThread().getName() + "业务办理结束，已离开");
                    } else {
                        System.out.println(Thread.currentThread().getName() + "选择不办理，傲娇的离开了");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }, "业务办理-" + i).start();
        }

    }

}

