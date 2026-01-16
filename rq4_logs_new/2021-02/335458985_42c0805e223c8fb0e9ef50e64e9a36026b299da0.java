package edu.cooper.ece366;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AppInClass {

  public static void main(String[] args) {
    // * --> "important" i.e. I use this a lot in my own job
    // Java Collection API
    // *Set -> *HashSet, TreeSet, LinkedHashSet, SortedSet ...
    // *List -> *ArrayList, LinkedList, Vector ...
    // *Map -> *HashMap, TreeMap, SortedMap ...
    List<String> list = List.of("a", "b", "c");
    Set<String> a = Set.of("a", "b");
    Map<String, Integer> a1 = Map.of("a", 1);

    List<String> objects = new ArrayList<>();
    System.out.println(objects.add("x"));
    System.out.println(objects.get(0));
    System.out.println(objects.contains("x"));

    // construct collections in various ways
    for (String s : list) {
      System.out.println("element : " + s);
    }
    for (int i = 0; i < list.size(); i++) {
      System.out.println("element at index " + i + ": " + list.get(i));
    }
    Consumer<String> action =
        new Consumer<>() {
          @Override
          public void accept(final String s) {
            System.out.println("element : " + s);
          }
        };
    list.forEach(action);

    MyInterface myInterface = i -> String.valueOf(i + i);
    MyInterface myInterface1 = String::valueOf;
    System.out.println(myInterface.getTwice(123));

    // some functional interfaces - Function, Consumer, Predicate, Supplier
    // You can implement any interface inline. Depending on the scope, you may want to declare in
    // its own class.
    // some lambdas - lambda body {} (with return), lambda, method reference
    Function<Integer, String> function = String::valueOf;
    Predicate<String> isEqualToYeet = s -> s.equals("yeet");
    System.out.println(isEqualToYeet.test("yeet"));
    Supplier<Long> clock = System::currentTimeMillis;

    // streams using map, filter, collect
    ArrayList<String> doubled = new ArrayList<>();
    for (String i : list) {
      doubled.add(i + i);
    }
    doubled.add("a");
    doubled.remove(0);

    List<String> doubledButWithStreams =
        list.stream()
            .filter(s -> !s.equals("a"))
            .flatMap(s -> Stream.of(s, s + s))
            .collect(Collectors.toList());
    List<String> collect =
        doubledButWithStreams.stream()
            .map(
                s -> {
                  if (s.length() > 1) {
                    return s.substring(0, 1);
                  } else {
                    return s;
                  }
                })
            .distinct()
            .collect(Collectors.toList());
    System.out.println(collect);

    // collect to map, groupby
    Map<String, String> stringStringMap =
        list.stream().collect(Collectors.toMap(s -> s, s -> s + s));
    System.out.println(stringStringMap);

    // map entryset
    Map<String, String> stringStringMap1 = stringStringMap.entrySet().stream()
        .filter(entry -> !entry.getKey().equals("a"))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    // threads (implements Runnable vs extends Thread)
    // start multiple threads, demo out of order
    Thread thread1 = new Thread(() -> {
      IntStream.range(0, 10).mapToObj(i -> "elem: " + i)
          .forEach(System.out::println);
//      try {
//        Thread.sleep(30_000L);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
    }, "thread-1");
    Runnable runnable = () -> {
      IntStream.range(0, 20).mapToObj(i -> "elem: " + i).forEach(System.out::println);
    };
    Thread thread2 =
        new Thread(
            runnable,
            "thread-2");
    thread1.start();
    thread2.start();

    System.out.println("hello world");

    // thread pool, executors, futures
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    Future<Integer> fiveFuture = executorService.submit(() -> 5);
    try {
      fiveFuture.get(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }

    // async - cleaner interfaces
    // CompletableFuture / CompletionStage (java interfaces)
    // ListenableFuture / ApiFuture (google interfaces)

    // I/O / networking --> typically you would use async ops
    CompletableFuture<Integer> integerCompletableFuture = CompletableFuture
        .supplyAsync(() -> 5, executorService);
    CompletableFuture<String> stringCompletableFuture = integerCompletableFuture
        .thenApply(i -> String.valueOf(i));
    CompletableFuture<String> stringCompletableFuture1 = integerCompletableFuture.thenCompose(i -> {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return CompletableFuture.completedFuture(String.valueOf(i));
    });

    // sockets / http


  }
}