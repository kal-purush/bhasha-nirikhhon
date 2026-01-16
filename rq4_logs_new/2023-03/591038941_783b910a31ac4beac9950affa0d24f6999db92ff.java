package com.ua.tsaran.homework18;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
;
public class Main {
    public static void main(String[] args) {
        List<Integer> tenList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tenList.add(i);
        }

        List<Integer> twoMillionList = new LinkedList<>();
        for (int i = 0; i < 2000000; i++) {
            twoMillionList.add(i);
        }

        System.out.println("10 елементів: " + tenList);
        System.out.println("2 000 000 елементів: "+ twoMillionList);

        List<Student> students = new ArrayList<>();
        students.add(new Student("Alice", 19));
        students.add(new Student("Stacy", 24));
        students.add(new Student("Billy", 20));
        students.add(new Student("Oleg", 22));
        students.add(new Student("Mike", 17));
        students.add(new Student("Jake", 18));


        System.out.println("Перелік студентів:");
        for (Student student : students) {
            System.out.println(student);
        }
    }
}