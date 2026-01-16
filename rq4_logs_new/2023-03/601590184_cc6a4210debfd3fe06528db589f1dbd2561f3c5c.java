package com.ua.robot.lesson18;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();

        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);

        List<Double> doubles = new ArrayList<>();

        for (int i = 0; i < list.size(); i++) {
            String stringValue = String.valueOf(list.get(i));
            double doubleValue = Double.parseDouble(stringValue);
            doubles.add(doubleValue);
        }

        System.out.println(doubles);





        int a = list.get(0);
        System.out.println(a);
        String s = String.valueOf(a);

        List oldList = new ArrayList<>();
        oldList.add(1);

        int b = (int) oldList.get(0);
        System.out.println(b);

        List<Integer> l2 = new ArrayList<>();
        l2.add(1);
        List<Integer> l3 = new LinkedList<>();
        l3.add(45);

        l2.addAll(l3);

        System.out.println(l2);

        ArrayList<Integer> l4 = new ArrayList<>();
        l4.trimToSize();






//        System.out.println(list);
//        System.out.println(a);
//        System.out.println(list.size());
//
//        list.add(0,0);
//        System.out.println(list);
//
//        list.remove(2);
//        System.out.println(list);
//
//        list.remove(2);
//        System.out.println(list);
//
//        System.out.println(list.get(2));
//
//        System.out.println(list.contains(4));
//
//        System.out.println(list.indexOf(4));
//
//        for (int i = 0; i < list.size(); i++) {
//            System.out.println(list.get(i));
//        }
//
//        for (Integer integer : list) {
//            System.out.print(integer + " ");
//        }








    }

}