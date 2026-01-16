package com.ssu.commerce.book.service;

import com.ssu.commerce.book.dto.param.ChangeBookParamDto;
import com.ssu.commerce.book.model.Book;
import com.ssu.commerce.book.supplier.BookTestDataSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
public class BookServiceDistributedLockTest implements BookTestDataSupplier {
    @Autowired
    private BookService bookService;

    private static final GenericContainer<?> redisContainer = new GenericContainer<>(DockerImageName.parse("redis:6.2.5"))
            .withExposedPorts(6379);

    @BeforeAll
    public static void startContainer() {
        redisContainer.start();
        System.setProperty("spring.redis.host", redisContainer.getHost());
        System.setProperty("spring.redis.port", String.valueOf(redisContainer.getFirstMappedPort()));

    }

    @Test
    public void testChangeBookWithDistributedLock() throws InterruptedException {
        int numberOfThreads = 100;

        CountDownLatch countDownLatch = new CountDownLatch(100);

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(() -> {
                try {
                    ChangeBookParamDto paramDto = BookTestDataSupplier.getChangeBookParamDtoRandom((long) Math.random());
                    Book book = bookService.changeBook(paramDto);
                    assertEquals(paramDto.getPublishDate(), book.getPublishDate());
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        assertEquals(0L, countDownLatch.getCount());
    }
}