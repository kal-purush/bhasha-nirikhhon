package lesson_14_Collections_Framework.livecoding;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// В качестве дополнительного ДЗ улучшить данный бенчмарк.
public class BenchmarkService {
    List<Integer> list;

    public BenchmarkService(List<Integer> list) {
        this.list = list;
    }

    public void add() {
            long start = System.nanoTime();

            for (int j = 0; j < 10_000_000; j++) {
                list.add(j);
            }

            long result = System.nanoTime() - start;

        System.out.println("Time to add: " + result / 1_000_000 + " ms");
    }

    public void get() {
        long start = System.nanoTime();

        for (int j = 0; j < 10_000_000; j++) {
            System.out.println(list.get(j));
        }

        long result = System.nanoTime() - start;

        System.out.println("Time to get: " + result / 1_000_000 + " ms");
    }

    public static void main(String[] args) {
    BenchmarkService benchmarkService = new BenchmarkService(new LinkedList<>());
    benchmarkService.add();
    benchmarkService.get();
    }
}

// ArrayList: add() with capacity: 200 ms
// ArrayList: add() default capacity: 320 ms
// LinkedList: add() - 1720 ms