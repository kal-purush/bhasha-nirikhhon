package com.huiyuan2.cloud.basic.utils;

import com.huiyuan2.cloud.basic.network.DefaultThread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description:
 * @author： 灰原二
 * @date: 2022/9/28 22:41
 */
public class NamedThreadFactory implements ThreadFactory {
    private boolean daemon;

    private String prefix;

    private AtomicInteger threadId = new AtomicInteger();

    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.daemon = daemon;
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new DefaultThread(prefix + threadId.getAndIncrement(), daemon, r);
    }
}