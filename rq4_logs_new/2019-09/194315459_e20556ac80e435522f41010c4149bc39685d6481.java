package com.lern.homeWork.lesson7.linkedList;

import java.util.Arrays;
import java.util.Iterator;

public class MainLinkedList {

    public static void main(String arr[]) {

        MyLinkedList<Integer> myLinkedList = new MyLinkedList();
        myLinkedList.add(1);
        myLinkedList.add(2);
        myLinkedList.add(3);
        myLinkedList.add(4);
        myLinkedList.add(5);
        myLinkedList.add(6);
        myLinkedList.add(7);
        myLinkedList.add(8);
        myLinkedList.add(10);
        myLinkedList.add(0,0);
        myLinkedList.add(9,9);

        System.out.println(myLinkedList.toString());
        System.out.println(myLinkedList.size());
        System.out.println(myLinkedList.isEmpty());

        System.out.println(Arrays.toString(myLinkedList.toArray()));

        Iterator<Integer> iter = myLinkedList.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }

        System.out.println(myLinkedList.contains(8));
        System.out.println(myLinkedList.contains(11));

        System.out.println("#1");
        System.out.println(myLinkedList.get(2));
        System.out.println(myLinkedList.get(5));

        System.out.println("#2");
        System.out.println(myLinkedList.set(4,10));
        System.out.println(myLinkedList.set(7,10));
        System.out.println(myLinkedList.toString());

        System.out.println("#3");
        myLinkedList.remove(2);
        System.out.println(myLinkedList.toString());
        System.out.println(myLinkedList.size());

        System.out.println("#4");
        System.out.println(myLinkedList.remove(new Integer(5)));
        System.out.println(myLinkedList.toString());
        System.out.println(myLinkedList.size());

        System.out.println("#5");
        System.out.println(myLinkedList.add(50));
        System.out.println(myLinkedList.toString());

        System.out.println("#6");
        System.out.println(myLinkedList.indexOf(new Integer(10)));
        System.out.println(myLinkedList.toString());
        System.out.println(myLinkedList.size());

        System.out.println("#7");
        System.out.println(myLinkedList.lastIndexOf(new Integer(10)));
        System.out.println(myLinkedList.toString());
        System.out.println(myLinkedList.size());

        myLinkedList.clear();
        System.out.println(myLinkedList.isEmpty());
    }
}