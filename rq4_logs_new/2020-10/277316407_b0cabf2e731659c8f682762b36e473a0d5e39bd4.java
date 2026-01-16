package ru.otus.cachehw;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class MyCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(MyCacheTest.class);

    //-Xms256m
    //-Xmx256m
    @Test
    public void weakReferenceDemo() throws InterruptedException {
        MyCache<String, Data> cache = new MyCache<>();
        for (int i = 0; i < 10000; i++) {
            cache.put("key " + i + 1, new Data());
        }
        logger.info("size before gc: {}", cache.size());
        System.gc();
        Thread.sleep(100);
        logger.info("size after gc: {}", cache.size());
    }

    private static class Data {

        private Double v1 = 1.0;

        private Double v2 = 2.0;

        private Double v3 = 3.0;

    }

}