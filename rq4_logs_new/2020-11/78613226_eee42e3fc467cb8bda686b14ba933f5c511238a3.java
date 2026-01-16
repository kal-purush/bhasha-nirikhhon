package com.giant.controller;


import java.util.stream.Stream;

public class StreamTest {

    public static void main(String[] args) {
     /*   Arrays.asList(1, 2, 3)
                .stream()
                .map(x -> x*x)
                .filter(x -> x>=5)
                .forEach(System.out::println);

        Stream.of("d2", "a2", "b1", "b3", "c")
                .filter(s -> {
                    System.out.println("filter: " + s);
                    return s.startsWith("a");
                }).forEach(System.out::println);*/

        Stream.of("d2", "a2", "b1", "b3", "c")
                .filter(s -> {
                    s.startsWith("a");
                    System.out.println("filter: " + s);
                    return true;
                }).forEach(System.out::println);
    }
}