package com.ua.robot.lesson19;

import java.text.CollationKey;
import java.text.Collator;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        LinkedList<Integer>[] l1 = new LinkedList[3];
        l1[0] = new LinkedList<>();
        l1[0].add(1);
        l1[0].add(11);


        Set<String> s1 = new HashSet<>();
        s1.add("1lklklklklklklklkk");
        s1.add("dfsds");
        s1.add("dgsdfgdfgh");
        s1.add("dgsdfgdfgh");

        System.out.println(s1);

        Set<String> s2 = new LinkedHashSet<>();
        s2.add("1lklklklklklklklkk");
        s2.add("dfsds");
        s2.add("dgsdfgdfgh");
        s2.add("dgsdfgdfgh");

        System.out.println(s2);

        s2.forEach(s -> {
            System.out.print(s + " ");
        });

        Collection<String> s3 = new LinkedHashSet<>();

        Person p1 = new Person(1, "David");
        Person p2 = new Person(11, "James");
        Person p3 = new Person(33, "Adam");

        List<Person> ps = new ArrayList<>();
        ps.add(p1);
        ps.add(p2);
        ps.add(p3);

        System.out.println(ps);
        Collections.sort(ps, new PersonAgeComparator());
        System.out.println(ps);


        ps.sort(new Comparator<Person>() {
            @Override
            public int compare(Person o1, Person o2) {
                Integer age1 = o1.getAge();
                Integer age2 = o2.getAge();
                return age1.compareTo(age2);
            }
        });

        Set<Person> pts = new TreeSet<>(ps);
        System.out.println(ps);
        pts.add(new Person(4, "Aaron"));

        System.out.println(pts);



    }
}