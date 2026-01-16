package com.ua.robotdreams.homework17;

import java.util.*;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        Random random = new Random();
        List<Integer> smallList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            smallList.add(random.nextInt(1,1000));
        }
        System.out.println(smallList.size());


        List<Integer> largeList = new LinkedList<>();
        for (int i = 0; i < 2000000; i++) {
            largeList.add(random.nextInt(1000));
        }
        System.out.println(largeList.size());


        List<Student> students = new ArrayList<>();
        students.add(new Student("Maria Koval", 19, "Fine Arts", 3.6));
        students.add(new Student("Vitaliy Baranov", 22, "Computer Science", 3.2));
        students.add(new Student("Sergiy Chernenko", 20, "Law", 4.2));
        students.add(new Student("Vasyl Ivanov", 24, "Philology", 3.3));
        for (Student student : students) {
            System.out.println(student);
        }
    }
}