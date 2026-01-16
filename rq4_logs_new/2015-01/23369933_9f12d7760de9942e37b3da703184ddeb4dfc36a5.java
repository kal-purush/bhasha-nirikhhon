package chapter6.exercise02;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int THREAD_NUM = 100;
    private static final int JOB_NUM = 10_000_000;

    /*
     * LongAdder と Atomic ロングの性能比較
     * IDを生成するだけのジョブを複数スレッドで実行した場合どちらが速いか
     * 結果:
     *   スレッドが多い(1000)と LongAdder のほうが速く、逆にスレッドが少ない(10)と AtomicLong のほうが早かった
     */
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        IDCreator idCreatorWithLongAdder = new IDCreatorWithLongAdder();
        Duration timeElapsedWithLongAdder = benchmark(() -> {
            for (int i = 0; i < JOB_NUM; i++) {
                Job job = new Job(idCreatorWithLongAdder);
                executorService.execute(job);
            }
        });
        System.out.println("Time elapsed with LongAdder: " + timeElapsedWithLongAdder.toMillis());

        IDCreator idCreatorWithAtomicLong = new IDCreatorWithAtomicLong();
        Duration timeElapsedWithAtomicLong = benchmark(() -> {
            for (int i = 0; i < JOB_NUM; i++) {
                Job job = new Job(idCreatorWithAtomicLong);
                executorService.execute(job);
            }
        });
        System.out.println("Time elapsed with AtomicLong: " + timeElapsedWithAtomicLong.toMillis());
    }

    public static Duration benchmark(Runnable measuredJob) {
        Instant start = Instant.now();
        measuredJob.run();
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        return timeElapsed;
    }

    /*
     * IDCreator から値を取得するだけのジョブ
     */
    static class Job implements Runnable {
        private IDCreator idCreator;

        Job(IDCreator idCreator) {
            this.idCreator = idCreator;
        }

        @Override
        public void run() {
            idCreator.getId();
        }
    }

}