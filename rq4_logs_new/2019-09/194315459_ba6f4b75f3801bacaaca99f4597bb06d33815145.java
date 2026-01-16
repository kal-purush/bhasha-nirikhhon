package com.lern.homeWork.lesson7.hashSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class MainHashSet {

    public static void main(String[] args) {

        Set<Integer> myHashSet = new MyHashSet<Integer>();
        myHashSet.add(5);
        myHashSet.add(12);
        myHashSet.add(23);
        myHashSet.add(34);
        myHashSet.add(45);
        myHashSet.add(56);
        myHashSet.add(67);
        myHashSet.add(78);
        myHashSet.add(89);
        myHashSet.add(90);
        myHashSet.add(107);
        myHashSet.add(118);
        myHashSet.add(129);
        myHashSet.add(132);
        myHashSet.add(143);
        myHashSet.add(154);
        myHashSet.add(165);
        myHashSet.add(176);
        myHashSet.add(187);
        myHashSet.add(198);

        System.out.println("Iterating over list:");
        Iterator<Integer> i = myHashSet.iterator();
        while (i.hasNext()) {
            System.out.print(i.next() + "  ");
        }
        System.out.println();

        System.out.println("size: " + myHashSet.size());

        System.out.println(myHashSet.add(34));
        System.out.println("size: " + myHashSet.size());

        System.out.println("is empty: " + myHashSet.isEmpty());

        System.out.println("List contains 45 or not: " +
                myHashSet.contains(45));

        System.out.println("List contains 55 or not: " +
                myHashSet.contains(55));

        System.out.println(myHashSet.remove(89));
        System.out.println("List after removing 89:");
        i = myHashSet.iterator();
        while (i.hasNext()) {
            System.out.print(i.next() + "  ");
        }
        System.out.println();

        System.out.println(myHashSet.remove(66));

        System.out.println("size: " + myHashSet.size());

        System.out.println(Arrays.toString(myHashSet.toArray()));

        myHashSet.clear();
        System.out.println(myHashSet.isEmpty());

    }
}