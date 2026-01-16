package ru.sber.Stream.test;

import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class StreamClassTest {

    @Test
    public void changeListTest() {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        System.out.println(list);

        list = list.stream().limit(list.size() - 3).collect(Collectors.toList());
        System.out.println(list);

        double newList = list
                .stream()
                .map(elem -> (elem % 2 == 0) ? elem + 5 : elem - 5)
                .mapToInt(elem -> elem)
                .average()
                .getAsDouble();
        System.out.println(newList);
    }

    @Test
    public void changeSetAndCreateMapTest() {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            set.add("abc".repeat(i + 1));
        }
        System.out.println(set);
        Map<Integer, String> map = set
                .stream()
                .filter(elem -> elem.length() > 10)
                .collect(Collectors.toMap(String::length, Function.identity()));
        System.out.println(map);
    }

    @Test
    public void createMapTest() {
        Map<String, String> newMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            newMap.put("aa".repeat(i + 1), "bb".repeat(i + 1));
        }
        System.out.println(newMap);

        List<String> listMap = newMap
                .entrySet()
                .stream()
                .map(elem -> elem.getKey() + "==" + elem.getValue())
                .collect(Collectors.toList());
        listMap.forEach(elem -> System.out.print(elem + " "));
        System.out.println();

        listMap = newMap
                .entrySet()
                .stream()
                .flatMap(elem -> Stream.of(elem.getKey(), elem.getValue()))
                .collect(Collectors.toList());
        System.out.println(listMap);
    }

    @Test
    public void createInstance() {
        assertNotNull(StreamClass.createInstance(new StreamClass()));
    }
}