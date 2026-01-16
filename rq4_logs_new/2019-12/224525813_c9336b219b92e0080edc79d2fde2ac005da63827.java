package lab;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StopWatch;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

@SpringBootTest
class CacheTest {

    @Autowired
    private TimeService timeService;

    @Test
    public void test() throws InterruptedException {
        long time = getTime();
        assertThat(time, greaterThanOrEqualTo(500L));
        time = getTime();
        assertThat(time, lessThan(500L));
        Thread.sleep(1000);
        time = getTime();
        assertThat(time, greaterThanOrEqualTo(500L));
    }

    private long getTime() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        timeService.getTime("key");
        stopWatch.stop();
        System.out.println(String.format("Time: %d", stopWatch.getLastTaskTimeMillis()));
        return stopWatch.getLastTaskTimeMillis();
    }

}