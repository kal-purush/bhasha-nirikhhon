package lesson_14_Collections_Framework.extra_task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;

public class BenchmarkMain {
    public static void main(String[] args) {
        BenchmarkService benchmarkService = new BenchmarkService();

        // Benchmark test for List or Set implementations
        benchmarkService.benchListOrSet(new ArrayList<>(), "ArrayList");
        benchmarkService.benchListOrSet(new LinkedList<>(), "LinkedList");
        benchmarkService.benchListOrSet(new HashSet<>(), "HashSet");
        benchmarkService.benchListOrSet(new TreeSet<>(), "TreeSet");

        // Benchmark test for Map implementations
        benchmarkService.benchMap(new HashMap<>(), "HashMap");
        benchmarkService.benchMap(new TreeMap<>(), "TreeMap");

        // Custom tests
        benchmarkService.insert(new HashMap<>(), "HashMap", 10_000_000);
        benchmarkService.insert(new LinkedHashSet<>(), "LinkedHashSet", 10_000_000);
        benchmarkService.search(benchmarkService.getCollection(), "LinkedHashSet", 100_000);
        benchmarkService.remove(benchmarkService.getMap(), "HashMap", 10_000);
        benchmarkService.remove(benchmarkService.getCollection(), "LinkedHashSet", 15_000);
    }
}

/**
 * OUTPUT:
 *  ===== ArrayList =====
 * ArrayList, insertion time for 100000 elements: 5 ms
 * ArrayList, search time for 10000 random searches: 1090 ms
 * ArrayList, remove time for 10000 elements: 97 ms
 *
 *
 *  ===== LinkedList =====
 * LinkedList, insertion time for 100000 elements: 2 ms
 * LinkedList, search time for 10000 random searches: 2606 ms
 * LinkedList, remove time for 10000 elements: 1 ms
 *
 *
 *  ===== HashSet =====
 * HashSet, insertion time for 100000 elements: 19 ms
 * HashSet, search time for 10000 random searches: 53 ms
 * HashSet, remove time for 10000 elements: 1 ms
 *
 *
 *  ===== TreeSet =====
 * TreeSet, insertion time for 100000 elements: 25 ms
 * TreeSet, search time for 10000 random searches: 45 ms
 * TreeSet, remove time for 10000 elements: 4 ms
 *
 *
 *  ===== HashMap =====
 * HashMap, insertion time for 100000 elements: 8 ms
 * HashMap, search time for 10000 random searches: 47 ms
 * HashMap, remove time for 10000 elements: 1 ms
 *
 *
 *  ===== TreeMap =====
 * TreeMap, insertion time for 100000 elements: 14 ms
 * TreeMap, search time for 10000 random searches: 48 ms
 * TreeMap, remove time for 10000 elements: 2 ms
 *
 * HashMap, insertion time for 10000000 elements: 695 ms
 * LinkedHashSet, insertion time for 10000000 elements: 988 ms
 * LinkedHashSet, search time for 100000 random searches: 426 ms
 * HashMap, remove time for 10000 elements: 0 ms
 *
 * LinkedHashSet - Remove, remove time for 15000 elements: 1 ms
 */