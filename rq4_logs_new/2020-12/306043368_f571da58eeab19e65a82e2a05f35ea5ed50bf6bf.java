package org.example.common.util;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author walle
 * @version V1.0
 * @since 2020-12-10 21:25
 */
public class TreadUtil {

    /**
     * 线程工厂
     */
    private static class NamingThreadFactory implements ThreadFactory {
        private static AtomicInteger threadCounter = new AtomicInteger(1);

        /**
         * 线程名称前缀
         */
        private String prefix;

        public NamingThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = prefix + "-" + threadCounter.getAndIncrement();
            return new Thread(r, threadName);
        }
    }

    /**
     * 创建固定线程数线程池
     *
     * @param poolName   池名称
     * @param threadSize 线程数
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor fixed(String poolName, int threadSize) {
        return create(poolName, threadSize, threadSize);
    }

    /**
     * 创建线程池
     *
     * @param poolName 线程池名称
     * @param coreSize coreSize
     * @param maxSize  maxSize
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor create(String poolName, int coreSize, int maxSize) {
        if (null == poolName || "".equals(poolName)) {
            poolName = "Pool" + DateUtil.formatNow(DateUtil.DATE_FORMAT);
        }
        NamingThreadFactory threadFactory = new NamingThreadFactory(poolName);
        return new ThreadPoolExecutor(coreSize, maxSize, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            threadFactory);
    }

    /**
     * 关闭线程池
     *
     * @param executor 线程池
     */
    public static void shutdown(ThreadPoolExecutor executor) {
        if (Objects.isNull(executor)) {
            return;
        }
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
    }
}