package executor;

import java.util.concurrent.*;

/**
 * @description:线程池
 * @auth tongchenghao
 * @date 2020/2/6
 */
public class ExecutorDemo {

    public static void main(String[] args) {
        // 创建一个不限制最大线程数的线程池
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        // 创建一个固定线程数的线程池，当前线程数为5
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
        // 创建一个只有一个线程的线程池
        ExecutorService singleThreadPool = Executors.newSingleThreadExecutor();
        // 创建一个延时执行的线程池
        ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(200);

        /**
         * 以上几种线程池都通过修改ThreadPoolExecutor参数来实现的，具体看源码，我们也可以自己实现自己需要的线程池
         * 参数说明：
         *      corePoolSize:核心线程数
         *      maximumPoolSize：最大线程数
         *      keepAliveTime：超时时间：超出核心线程数以外的线程的空余线程的存活时间
         *      TimeUnit：存活时间的单位
         *      workQueue：阻塞队列
         *      threadFactory：线程池创建线程时使用的线程工厂
         *      handler：reject操作，拒绝操作
         */
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(0, 100, 60L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0;i<20;i++){
            int finalI = i;
            Thread thread = new Thread(() -> {
                System.out.println("进入线程" + finalI);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println("线程" + finalI + "结束");
            });
            // 一次最多同时执行5个线程
            fixedThreadPool.execute(thread);
        }
        fixedThreadPool.shutdown();

    }

}