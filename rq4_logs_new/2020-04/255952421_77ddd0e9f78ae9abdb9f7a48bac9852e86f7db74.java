package com.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

/**
 *
 * @Date 2020/4/26 19:08
 * @name CyclicBarrierDemo
 */


public class CyclicBarrierDemo {

    //存放子线程工作结果的容器
    private static ConcurrentHashMap<String,Long> resultMap
            = new ConcurrentHashMap<>();

    //可以接收多个线程运行后的汇总任务
    private static final CyclicBarrier barrier=new CyclicBarrier(4,new CollectThread());
    private static class CollectThread implements Runnable{

        @Override
        public void run() {
            StringBuilder sb=new StringBuilder();
            for(Map.Entry<String,Long> workResult:resultMap.entrySet()){
                sb.append("["+workResult.getValue()+"]");
            }
            System.out.println(" the result = "+ sb);
            System.out.println("do other business........");
        }
    }
    private static class SubThread implements Runnable{

        @Override
        public void run() {
            long id = Thread.currentThread().getId();
            resultMap.put(Thread.currentThread().getId()+"",id);
            try {
                Thread.sleep(1000+id);
                System.out.println("Thread_"+id+" ....do something ");
                barrier.await();
                Thread.sleep(1000+id);
                System.out.println("Thread_"+id+" ....do its business ");
                //多次使用 await() 计数器
                barrier.await();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
        for(int i=0;i<=4;i++){
            Thread thread = new Thread(new SubThread());
            thread.start();
        }
    }
}