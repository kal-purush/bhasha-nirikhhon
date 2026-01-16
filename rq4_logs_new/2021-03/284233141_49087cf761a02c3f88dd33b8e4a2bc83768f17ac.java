package com.github.dolly0526;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.Suspendable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author yusenyang
 * @create 2021/3/28 19:52
 */
@Slf4j
public class TestQuasar {

    private JdbcTemplate jdbcTemplate = new JdbcTemplate(
            new DriverManagerDataSource(
                    "jdbc:mysql://localhost:3306/vvms",
                    "root",
                    "root"
            )
    );


    @Test
    public void testFiber() throws ExecutionException, InterruptedException, IOException {

        Fiber<Void> fiber1 = new Fiber<Void>(() -> {
            for (int i = 0; i < 10; i++) {

                System.out.println(Thread.currentThread().getName() + " " +
                        Fiber.currentFiber().getName() + " " +
                        i);

                testSleep(1);
            }
        }).start();

        Fiber<Void> fiber2 = new Fiber<Void>(() -> {
            for (int i = 0; i < 10; i++) {

                System.out.println(Thread.currentThread().getName() + " " +
                        Fiber.currentFiber().getName() + " " +
                        i);

                testSleep(2);
            }
        }).start();

        Thread.sleep(21000);
    }


    @SneakyThrows
    @Suspendable
    private void testSleep(long seconds) {
//        jdbcTemplate.execute("SELECT SLEEP(" + seconds + ")");
        Fiber.sleep(seconds * 1000);
    }
}