package com.imooc.spring.reactive.loader;

import java.util.concurrent.CompletableFuture;

import static java.lang.System.out;

/**
 * @Auther: langdylan
 * @Date: 2019-08-29 11:09
 * @Description:
 */
public class ChainDataLoader extends DataLoader {
    /**
     * [Thread: ForkJoinPool.commonPool-worker-3] loadConfigurations() costs: 1005 ms
     * [Thread: ForkJoinPool.commonPool-worker-3] loadUsers() costs: 2004 ms
     * [Thread: ForkJoinPool.commonPool-worker-3] loadOrders costs: 3004 ms
     * [Thread: ForkJoinPool.commonPool-worker-3] Load completed.
     * load() total cost: 6072 ms
     *
     * @param args
     */

    public static void main(String[] args) {
        new ChainDataLoader().load();
    }

    @Override
    protected void doLoad() {
        CompletableFuture
                .runAsync(super::loadConfigurations)// 耗时 >= 1s
                .thenRun(super::loadUsers)// 耗时 >= 2s
                .thenRun(super::loadOrders)// 耗时 >= 3s
                .whenComplete((result, throwable) -> {//完成回掉
                    out.printf("[Thread: %s] Load completed.\n", Thread.currentThread().getName());
                })
                .join();//等待完成
    }
}