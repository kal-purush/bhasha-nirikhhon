package com.nc.ocp.concurrency.test;

import lombok.extern.log4j.Log4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Log4j
public class SyncTester {

    public void run() {
        syncTest(new SheepManager());
        syncTest(new AtomicSheepManager());
    }

    private void syncTest(AnimalManager manager) {
        ExecutorService service = null;

        try {
            service = Executors.newFixedThreadPool(20);
            for (int i = 0; i < 10; i++) {
                service.submit(manager::incrementAndReport);
            }
        } finally {
            if (service != null) service.shutdown();
        }
        if (service != null) {
            try {
                while (!service.isTerminated()) {
                    log.info("At least one task is still running.");
                    service.awaitTermination(1, TimeUnit.SECONDS);
                }
                log.info("All task are finished.");
                log.info("[" + manager.getClass().getSimpleName() + "] Sheep count result: " + manager.getResult());
            } catch (InterruptedException e) {
                log.error("Wait thread was interrupted: " + e.getLocalizedMessage(), e);
            }
        }
    }

    private void syncTest() {

    }

    private interface AnimalManager {
        void incrementAndReport();
        String getResult();
    }

    private static class SheepManager implements AnimalManager {

        private int count = 0;
        private String result = "";

        @Override
        public String getResult() {
            return result;
        }

        @Override
        public void incrementAndReport() {
            result += (++count) + " ";
        }
    }


    private static class AtomicSheepManager implements AnimalManager {

        private AtomicInteger count = new AtomicInteger(0);
        private AtomicReference<String> result = new AtomicReference<>("");

        @Override
        public String getResult() {
            return result.get();
        }

        @Override
        public void incrementAndReport() {
            result.set(result.get() + count.incrementAndGet() + " ");
        }
    }
}