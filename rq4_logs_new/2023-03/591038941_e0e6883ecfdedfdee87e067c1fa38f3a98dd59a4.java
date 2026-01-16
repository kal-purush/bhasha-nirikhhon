package com.ua.tsaran.homework19;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class Main {
    public static void main(String[] args) {
        Random random = new Random();

        Set<Integer> hashSet = new HashSet<>();
        Set<Integer> treeSet = new TreeSet<>();
        Set<Integer> linkedHashSet = new LinkedHashSet<>();

        for (int i = 0; i < 1000; i++) {
            int number = random.nextInt(1, 50);
            hashSet.add(number);
            treeSet.add(number);
            linkedHashSet.add(number);
        }

        System.out.println("HashSet:");
        for (int number : hashSet) {
            System.out.print(number + " ");
        }
        System.out.println();

        System.out.println("TreeSet:");
        for (int number : treeSet) {
            System.out.print(number + " ");
        }
        System.out.println();

        System.out.println("LinkedHashSet:");
        for (int number : linkedHashSet) {
            System.out.print(number + " ");
        }
        System.out.println();
    }
}