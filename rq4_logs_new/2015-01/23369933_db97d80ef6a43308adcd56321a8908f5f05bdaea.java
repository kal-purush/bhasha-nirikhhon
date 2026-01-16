package chapter6.exercise03;

import static utils.BenchMark.benchmark;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int THREAD_NUM = 1000;
    private static final int COUNT_NUM = 100_000;

    public static void main(String[] args) {
        Thread[] threads = new Thread[THREAD_NUM];
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);

        Counter counterWithAtomicLong = new CounterWithAtomicLong();
        Duration timeElapsedWithAtomicLong = benchmark(() -> {
            for (int i = 0; i < THREAD_NUM; i++) {
                CountUpJob job = new CountUpJob(counterWithAtomicLong, COUNT_NUM);
                threads[i] = new Thread(job);
                threads[i].start();
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
        });
        System.out.println("Time elapsed with AtomicLong: " + timeElapsedWithAtomicLong.toMillis());

        Counter counterWithLongAdder = new CounterWithLongAdder();
        Duration timeElapsedWithLongAdder = benchmark(() -> {
            for (int i = 0; i < THREAD_NUM; i++) {
                CountUpJob job = new CountUpJob(counterWithLongAdder, COUNT_NUM);
                threads[i] = new Thread(job);
                threads[i].start();
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
        });
        System.out.println("Time elapsed with LongAdder: " + timeElapsedWithLongAdder.toMillis());
    }

    static class CountUpJob implements Runnable {
        private Counter counter;
        private int countUpNum;

        public CountUpJob(Counter counter, int countUpNum) {
            this.counter = counter;
            this.countUpNum = countUpNum;
        }

        @Override
        public void run() {
            for (int i = 0; i < this.countUpNum; i++) {
                this.counter.countUp();
            }
        }
    }

}