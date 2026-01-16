package com.github.codeboyzhou.mcp.declarative.server.register;

import com.github.codeboyzhou.mcp.declarative.util.NamedThreadFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class ComponentBufferQueue<T, R> {

    private static final int DEFAULT_DELAYED_CONSUMPTION_MILLIS = 10;

    private final BlockingQueue<R> queue = new LinkedBlockingQueue<>();

    private final long delayMillis;

    public ComponentBufferQueue(long delayMillis) {
        if (delayMillis <= 0) {
            throw new IllegalArgumentException("delayMillis must be greater than 0");
        }
        this.delayMillis = delayMillis;
    }

    public ComponentBufferQueue() {
        this(DEFAULT_DELAYED_CONSUMPTION_MILLIS);
    }

    public void submit(R component) {
        try {
            queue.put(component);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void consume(T server, BiConsumer<T, R> consumer) {
        NamedThreadFactory threadFactory = new NamedThreadFactory(getClass().getSimpleName());
        Executors.newSingleThreadExecutor(threadFactory).execute(() -> {
            try {
                while (!Thread.interrupted()) {
                    R component = queue.take();
                    consumer.accept(server, component);
                    TimeUnit.MILLISECONDS.sleep(delayMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

}